mod de;
mod error;
mod liberror;
mod helpers;
mod ser;
mod funcs;
mod transform;

use neon::prelude::*;





#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("compile_vrl_program", transform::compile_vrl_program)?;
    cx.export_function("_vrl", transform::vrl)?;
    cx.export_function("vrl_function_info", funcs::vrl_function_info)?;
    Ok(())
}

