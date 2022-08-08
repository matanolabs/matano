export type Outcome = {
  success?: {
    output?: any;
    result: any;
  };
} & { error?: string };
export function vrl_function_info(): null;
export function _vrl(prog_id: string, event: any, tz: any): Outcome;
export function compile_vrl_program(program: string): Outcome;
