use anyhow::{anyhow, Result};
use arrow2::datatypes::*;
use arrow2::io::avro::avro_schema::schema::{
    BytesLogical, Field as AvroField, Fixed, FixedLogical, IntLogical, LongLogical, Record,
    Schema as AvroSchema,
};
use indexmap::map::IndexMap as HashMap;
use indexmap::set::IndexSet as HashSet;
use std::borrow::Borrow;
use log::{debug};

const ITEM_NAME: &str = "item";

/// Coerce an heterogeneous set of [`DataType`] into a single one. Rules:
/// * The empty set is coerced to `Null`
/// * `Int64` and `Float64` are `Float64`
/// * Lists and scalars are coerced to a list of a compatible scalar
/// * Structs contain the union of all fields
/// * All other types are coerced to `Utf8`
pub(crate) fn coerce_data_type<A: Borrow<DataType> + std::fmt::Debug>(datatypes: &[A]) -> DataType {
    use DataType::*;

    if datatypes.is_empty() {
        return DataType::Null;
    }

    // let are_all_equal = datatypes.windows(2).all(|w| w[0].borrow() == w[1].borrow());

    // if are_all_equal {
    //     return datatypes[0].borrow().clone();
    // }

    let are_all_structs = datatypes.iter().all(|x| matches!(x.borrow(), Struct(_)));

    if are_all_structs {
        // all are structs => union of all fields (that may have equal names)
        let fields = datatypes.iter().fold(vec![], |mut acc, dt| {
            if let Struct(new_fields) = dt.borrow() {
                acc.extend(new_fields);
            };
            acc
        });
        // group fields by unique
        let fields = fields.iter().fold(
            HashMap::<&String, HashSet<&DataType>>::new(),
            |mut acc, field| {
                match acc.entry(&field.name) {
                    indexmap::map::Entry::Occupied(mut v) => {
                        v.get_mut().insert(&field.data_type);
                    }
                    indexmap::map::Entry::Vacant(v) => {
                        let mut a = HashSet::new();
                        a.insert(&field.data_type);
                        v.insert(a);
                    }
                }
                acc
            },
        );
        // and finally, coerce each of the fields within the same name
        let fields = fields
            .into_iter()
            .map(|(name, dts)| {
                let dts = dts.into_iter().collect::<Vec<_>>();
                Field::new(name, coerce_data_type(&dts), true)
            })
            .filter(|f| f.data_type != DataType::Null)
            .collect::<Vec<_>>();

        let data_type =
         if fields.iter().filter(|f| f.data_type != DataType::Null).count() > 0 {
            Struct(fields)
        } else {
            debug!("datatypes: {:?} \n\n fields:{:?}",datatypes,fields);
            DataType::Null
        };
        return data_type;
    } else if datatypes.len() > 2 {
        return Utf8;
    } else if datatypes.len() == 1 {
        return datatypes[0].borrow().clone();
    }
    let (lhs, rhs) = (datatypes[0].borrow(), datatypes[1].borrow());

    return match (lhs, rhs) {
        (lhs, rhs) if lhs == rhs => lhs.clone(),
        (List(lhs), List(rhs)) => {
            let inner = coerce_data_type(&[lhs.data_type(), rhs.data_type()]);
            List(Box::new(Field::new(ITEM_NAME, inner, true)))
        }
        (scalar, List(list)) => {
            let inner = coerce_data_type(&[scalar, list.data_type()]);
            List(Box::new(Field::new(ITEM_NAME, inner, true)))
        }
        (List(list), scalar) => {
            let inner = coerce_data_type(&[scalar, list.data_type()]);
            List(Box::new(Field::new(ITEM_NAME, inner, true)))
        }
        (Float64, Int64) => Float64,
        (Int64, Float64) => Float64,
        (Int64, Boolean) => Int64,
        (Boolean, Int64) => Int64,
        (_, _) => Utf8,
    };
}

pub(crate) fn type_to_schema(
    data_type: &DataType,
    is_nullable: bool,
    name: String,
) -> Result<AvroSchema> {
    let schema = _type_to_schema(data_type, name)?;
    Ok(if is_nullable && schema != AvroSchema::Null {
        AvroSchema::Union(vec![AvroSchema::Null, schema])
    } else {
        schema
    })
}

fn field_to_field(field: &Field) -> Result<AvroField> {
    let schema = type_to_schema(field.data_type(), true, field.name.clone())?;
    Ok(AvroField {
        name: (&field.name).into(),
        doc: None,
        schema,
        default: Some(AvroSchema::Null),
        order: None,
        aliases: vec![],
    })
}

fn _type_to_schema(data_type: &DataType, name: String) -> Result<AvroSchema> {
    Ok(match data_type.to_logical_type() {
        DataType::Null => AvroSchema::Null,
        DataType::Boolean => AvroSchema::Boolean,
        DataType::Int32 => AvroSchema::Int(None),
        DataType::Int64 => AvroSchema::Long(None),
        DataType::Float32 => AvroSchema::Float,
        DataType::Float64 => AvroSchema::Double,
        DataType::Binary => AvroSchema::Bytes(None),
        DataType::LargeBinary => AvroSchema::Bytes(None),
        DataType::Utf8 => AvroSchema::String(None),
        DataType::LargeUtf8 => AvroSchema::String(None),
        DataType::LargeList(inner) | DataType::List(inner) => AvroSchema::Array(Box::new(
            type_to_schema(&inner.data_type, true, inner.name.clone())?,
        )),
        DataType::Struct(fields) => AvroSchema::Record(Record::new(
            name,
            fields
                .iter()
                .map(field_to_field)
                .collect::<Result<Vec<_>>>()?,
        )),
        DataType::Date32 => AvroSchema::Int(Some(IntLogical::Date)),
        DataType::Time32(TimeUnit::Millisecond) => AvroSchema::Int(Some(IntLogical::Time)),
        DataType::Time64(TimeUnit::Microsecond) => AvroSchema::Long(Some(LongLogical::Time)),
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            AvroSchema::Long(Some(LongLogical::LocalTimestampMillis))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            AvroSchema::Long(Some(LongLogical::LocalTimestampMicros))
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let mut fixed = Fixed::new("", 12);
            fixed.logical = Some(FixedLogical::Duration);
            AvroSchema::Fixed(fixed)
        }
        DataType::FixedSizeBinary(size) => AvroSchema::Fixed(Fixed::new("", *size)),
        DataType::Decimal(p, s) => AvroSchema::Bytes(Some(BytesLogical::Decimal(*p, *s))),
        other => return Err(anyhow!("write {:?} to avro", other)),
    })
}
