import path from 'path';

export {run} from '@oclif/core'
export const SRC_ROOT_DIR = __dirname;
export const CLI_ROOT_DIR = path.join(SRC_ROOT_DIR, "../");
export const PROJ_ROOT_DIR = path.join(CLI_ROOT_DIR, "../");
