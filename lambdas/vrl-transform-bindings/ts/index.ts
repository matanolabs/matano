import { _vrl, Outcome } from "./functions";
export { Outcome, vrl_function_info } from "./functions";

// export type Outcome =
//   | {
//       type: "success";
//       output: string;
//       result: any;
//     }
//   | { type: "error"; message: string };

export function vrl(program: string, event: any): Outcome {
  return _vrl(program, event, null);
}
