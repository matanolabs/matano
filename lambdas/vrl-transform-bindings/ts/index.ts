import { _vrl, Outcome, compile_vrl_program } from "./functions";
export { Outcome, vrl_function_info, compile_vrl_program } from "./functions";

const compiledProgramCache = new Map<string, string>();

export function vrl(program: string, event: any): Outcome {
  let progId: string | undefined;
  if (compiledProgramCache.has(program)) {
    progId = compiledProgramCache.get(program);
  } else {
    console.time("compile_vrl_program");
    const output = compile_vrl_program(program);
    console.timeEnd("compile_vrl_program");
    if (output.error != null) {
      return output;
    } else {
      progId = output.success!!.output;
      compiledProgramCache.set(program, progId!!);
    }
  }
  // console.time(`_vrl: ${progId}`);
  const res = _vrl(progId!!, event, null);
  // console.timeEnd(`_vrl: ${progId}`);
  return res;
}
