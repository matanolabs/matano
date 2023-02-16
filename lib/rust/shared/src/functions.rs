use ::value::Value;
use vrl::prelude::*;

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
    ) -> Compiled {;
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

pub fn custom_vrl_functions() -> Vec<Box<dyn vrl::Function>> {
    vec![
        Box::new(BitwiseAnd) as _,
    ]
}