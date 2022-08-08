use crate::{arg, args, helpers::*};

use lazy_static::lazy_static;
use neon::prelude::*;
use std::sync::Mutex;
use uuid::Uuid;

use ::value::{Secrets, Value};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use vector_common::TimeZone;
use vrl::{diagnostic::Formatter, state, value, Program, Runtime, TargetValueRef};

// An enum for the result of a VRL resolution operation
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    Success { output: Value, result: Value },
    Error(String),
}

lazy_static! {
    static ref RUNTIME: Mutex<Runtime> = Mutex::new(Runtime::new(state::Runtime::default()));
    static ref PROGRAMS: Mutex<BTreeMap<String, Program>> = Mutex::new(BTreeMap::new());
}

pub fn compile_vrl_program(mut cx: FunctionContext) -> JsResult<JsValue> {
    args! {
        cx,
        program: String
    }
    let mut state = state::ExternalEnv::default();
    let (program, _warnings) =
        match vrl::compile_with_external(&program, &vrl_stdlib::all(), &mut state) {
            Ok(program) => program,
            Err(diagnostics) => {
                let msg = Formatter::new(&program, diagnostics).to_string();
                return convert(&mut cx, &Outcome::Error(msg));
            }
        };
    let prog_id = Uuid::new_v4().to_string();
    PROGRAMS.lock().unwrap().insert(prog_id.clone(), program);
    convert(&mut cx, &Outcome::Success {
        output: Value::from(prog_id),
        result: Value::Null,
    })    
}

// The VRL resolution logic
pub fn vrl(mut cx: FunctionContext) -> JsResult<JsValue> {
    // The VRL program plus (optional) event plus (optional) time zone
    args! {
        cx,
        prog_id: String,
        event: Option<Value>,
        tz: Option<String>
    }
    let mut value: Value = event.unwrap_or(value!({}));

    // TODO: instantiate this logic elsewhere rather than for each invocation,
    // as these values are basically constants. This is fine for now, as
    // performance shouldn't be an issue in the near term, but low-hanging fruit
    // for optimization later.
    let mut runtime = RUNTIME.lock().unwrap();

    // Default to default timezone if none
    let time_zone_str = tz.unwrap_or_default();

    let time_zone = match TimeZone::parse(&time_zone_str) {
        Some(tz) => tz,
        None => TimeZone::Local,
    };

    // TODO return warnings too
    let programs = PROGRAMS.lock().unwrap();
    let program = programs.get(&prog_id).unwrap();
    
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
