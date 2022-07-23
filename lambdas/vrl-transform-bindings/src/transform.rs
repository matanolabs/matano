use crate::{helpers::*, arg, args,};

use neon::prelude::*;

use serde::{Serialize, Deserialize};
use ::value::{Secrets, Value};
use std::collections::BTreeMap;
use vector_common::TimeZone;
use vrl::{diagnostic::Formatter, state, value, Runtime, TargetValueRef};

// An enum for the result of a VRL resolution operation
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    Success { output: Value, result: Value },
    Error(String),
}

// The VRL resolution logic
pub fn vrl(mut cx: FunctionContext) -> JsResult<JsValue> {
    // The VRL program plus (optional) event plus (optional) time zone
    args! {
        cx,
        program: String,
        event: Option<Value>,
        tz: Option<String>
    }
    let mut value: Value = event.unwrap_or(value!({}));

    // TODO: instantiate this logic elsewhere rather than for each invocation,
    // as these values are basically constants. This is fine for now, as
    // performance shouldn't be an issue in the near term, but low-hanging fruit
    // for optimization later.
    let mut state = state::ExternalEnv::default();
    let mut runtime = Runtime::new(state::Runtime::default());

    // Default to default timezone if none
    let time_zone_str = tz.unwrap_or_default();

    let time_zone = match TimeZone::parse(&time_zone_str) {
        Some(tz) => tz,
        None => TimeZone::Local,
    };

    // TODO return warnings too
    let (program, _warnings) =
        match vrl::compile_with_external(&program, &vrl_stdlib::all(), &mut state) {
            Ok(program) => program,
            Err(diagnostics) => {
                let msg = Formatter::new(&program, diagnostics).to_string();
                return convert(&mut cx, &Outcome::Error(msg));
            }
        };

    let mut metadata = Value::Object(BTreeMap::new());
    let mut secrets = Secrets::new();
    let mut target = TargetValueRef {
        value: &mut value,
        metadata: &mut metadata,
        secrets: &mut secrets,
    };

    let res = match runtime.resolve(&mut target, &program, &time_zone) {
        Ok(result) => Outcome::Success {
            output: result,
            // TODO: figure out if i need the .clone() here, as VRL mutates the the event 
            result: value,
        },
        Err(err) => Outcome::Error(err.to_string()),
    };

    convert(&mut cx, &res)
}
