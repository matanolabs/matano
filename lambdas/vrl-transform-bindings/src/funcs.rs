use crate::{helpers::*,};

use neon::prelude::*;
use serde::{Deserialize, Serialize};
use std::convert::{From};

#[derive(Serialize, Debug, Deserialize)]
struct User {
    name: String,
    age: u16,
}

// VRL example
#[derive(Serialize)]
struct Example {
    title: &'static str,
    source: &'static str,
}

// VRL function parameter
#[derive(Serialize)]
struct Parameter {
    name: &'static str,
    kind: String,
    required: bool,
}

// VRL function info
#[derive(Serialize)]
struct Function {
    name: &'static str,
    parameters: Vec<Parameter>,
    examples: Vec<Example>,
}

// Convert a VRL Function struct into our custom Function type (to display as JSON)
impl From<&Box<dyn vrl::Function>> for Function {
    fn from(f: &Box<dyn vrl::Function>) -> Function {
        Function {
            name: f.identifier(),
            parameters: f
                .parameters()
                .iter()
                .map(|p| Parameter {
                    name: p.keyword,
                    kind: p.kind().to_string(),
                    required: p.required,
                })
                .collect(),
            examples: f
                .examples()
                .iter()
                .map(|e| Example {
                    title: e.title,
                    source: e.source,
                })
                .collect(),
        }
    }
}

pub fn vrl_function_info(mut cx: FunctionContext) -> JsResult<JsValue> {
    let functions: Vec<Function> = vrl_stdlib::all().iter().map(Function::from).collect();
    convert(&mut cx, &functions)
}



