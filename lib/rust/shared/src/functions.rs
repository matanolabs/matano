use std::collections::HashMap;

use ::value::Value;
use vrl::prelude::*;

use lazy_static::lazy_static;
use tracing::log::{error, info};

use crate::{
    enrichment::{create_enrichment_table, EnrichmentTable},
    load_enrichment_config,
};

lazy_static! {
    static ref ENRICHMENT_TABLES_MAP: HashMap<String, EnrichmentTable> = {
        let enrichment_table_configs = load_enrichment_config();
        let mut tables = HashMap::new();
        for (table_name, _) in enrichment_table_configs.unwrap() {
            let table = create_enrichment_table(&table_name);
            match table {
                Ok(table) => {
                    tables.insert(table_name, table);
                }
                Err(e) => {
                    error!("Error creating enrichment table: {}, {}", table_name, e)
                }
            }
        }
        tables
    };
}

fn bitwise_and(value: Value, other: Value) -> Resolved {
    let result = match (value, other) {
        (Value::Integer(value), Value::Integer(other)) => Value::Integer(value & other),
        _ => unreachable!(),
    };
    Ok(result)
}

#[derive(Clone, Copy, Debug)]
pub struct BitwiseAnd;

impl Function for BitwiseAnd {
    fn identifier(&self) -> &'static str {
        "bitwise_and"
    }

    fn parameters(&self) -> &'static [Parameter] {
        &[
            Parameter {
                keyword: "value",
                kind: kind::INTEGER,
                required: true,
            },
            Parameter {
                keyword: "other",
                kind: kind::INTEGER,
                required: true,
            },
        ]
    }

    fn examples(&self) -> &'static [Example] {
        &[Example {
            title: "bitwise_and",
            source: r#"bitwise_and(5, 3)"#,
            result: Ok("1"),
        }]
    }

    fn compile(
        &self,
        _state: &state::TypeState,
        _ctx: &mut FunctionCompileContext,
        mut arguments: ArgumentList,
    ) -> Compiled {
        let value = arguments.required("value");
        let other = arguments.required("other");
        // TODO: return a compile-time error if other is 0

        Ok(BitwiseAndFn { value, other }.as_expr())
    }
}

#[derive(Debug, Clone)]
struct BitwiseAndFn {
    value: Box<dyn Expression>,
    other: Box<dyn Expression>,
}

impl FunctionExpression for BitwiseAndFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let value = self.value.resolve(ctx)?;
        let other = self.other.resolve(ctx)?;
        r#bitwise_and(value, other)
    }

    fn type_def(&self, _: &state::TypeState) -> TypeDef {
        // Division is infallible if the rhs is a literal normal float or a literal non-zero integer.
        match self.other.as_value() {
            Some(value) if value.is_integer() => TypeDef::integer().infallible(),
            _ => TypeDef::integer().fallible(),
        }
    }
}

fn get_enrichment_table_record(
    select: Option<Value>,
    table: &str,
    condition: (Option<&str>, &str),
) -> Resolved {
    let select = select
        .map(|array| match array {
            Value::Array(arr) => arr
                .iter()
                .map(|value| Ok(value.try_bytes_utf8_lossy()?.to_string()))
                .collect::<std::result::Result<Vec<_>, _>>(),
            value => Err(vrl::value::Error::Expected {
                got: value.kind(),
                expected: Kind::array(Collection::any()),
            }),
        })
        .transpose()?;

    info!("getting by key: {:#?}", condition);

    let data = ENRICHMENT_TABLES_MAP
        .get(table)
        .ok_or("Enrichment table not found")?
        .get_by_key_internal(condition.1, condition.0)
        .map_err(|e| format!("failed to get enrichment table record {:#}", e))?;

    let data = match data {
        Some(data) => data.into(),
        None => Value::Null,
    };

    Ok(data)
}

#[derive(Clone, Copy, Debug)]
pub struct GetEnrichmentTableRecord;

impl Function for GetEnrichmentTableRecord {
    fn identifier(&self) -> &'static str {
        "get_enrichment_table_record"
    }

    fn parameters(&self) -> &'static [Parameter] {
        &[
            Parameter {
                keyword: "table",
                kind: kind::BYTES,
                required: true,
            },
            Parameter {
                keyword: "condition",
                kind: kind::OBJECT | kind::BYTES,
                required: true,
            },
            Parameter {
                keyword: "select",
                kind: kind::ARRAY,
                required: false,
            },
        ]
    }

    fn examples(&self) -> &'static [Example] {
        &[Example {
            title: "find records",
            source: r#"get_enrichment_table_record!("test", {"id": 1})"#,
            result: Ok(r#"{"id": 1, "firstname": "Bob", "surname": "Smith"}"#),
        }]
    }

    fn compile(
        &self,
        _state: &state::TypeState,
        ctx: &mut FunctionCompileContext,
        mut arguments: ArgumentList,
    ) -> Compiled {
        let tables = ENRICHMENT_TABLES_MAP
            .keys()
            .into_iter()
            .map(|v| Value::from(v.as_str()))
            .collect::<Vec<_>>();

        let table = arguments
            .required_enum("table", &tables)?
            .try_bytes_utf8_lossy()
            .expect("table is not valid utf8")
            .into_owned();
        let condition = arguments.required("condition");

        let select = arguments.optional("select");

        Ok(GetEnrichmentTableRecordFn {
            table,
            condition,
            select,
        }
        .as_expr())
    }
}

#[derive(Debug, Clone)]
pub struct GetEnrichmentTableRecordFn {
    table: String,
    condition: Box<dyn Expression>,
    select: Option<Box<dyn Expression>>,
}

impl FunctionExpression for GetEnrichmentTableRecordFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        // throw error if self.condition map has more than one key, otherwise unpack to lookup_key and lookup_value
        let condition = self.condition.resolve(ctx)?;
        let (lookup_key, lookup_value) =
            if condition.is_object() && condition.as_object().unwrap().len() == 1 {
                let (key, value) = condition.as_object().unwrap().iter().next().unwrap();
                (
                    Some(key.as_str()),
                    value
                        .as_str()
                        .ok_or("err invalid lookup value not str")?
                        .to_string(),
                )
            } else if condition.is_bytes() {
                (
                    None,
                    String::from_utf8_lossy(condition.as_bytes().unwrap()).to_string(),
                )
            } else {
                // throw error;
                return Err(ExpressionError::from(
                    "condition must be a string or object with exactly one key/value lookup pair",
                ));
            };

        let select = self
            .select
            .as_ref()
            .map(|array| array.resolve(ctx))
            .transpose()?;

        let table = &self.table;

        get_enrichment_table_record(select, table, (lookup_key, lookup_value.as_str()))
    }

    fn type_def(&self, _: &state::TypeState) -> TypeDef {
        TypeDef::object(Collection::any()).fallible()
    }
}

pub fn custom_vrl_functions() -> Vec<Box<dyn vrl::Function>> {
    vec![
        Box::new(BitwiseAnd) as _,
        Box::new(GetEnrichmentTableRecord) as _,
    ]
}
