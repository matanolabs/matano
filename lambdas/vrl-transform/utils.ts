import { Outcome } from "@matano/vrl-transform-bindings";

import * as zlib from "zlib";
import * as readline from "readline";

export type ErrType<E> = { ok: false, error: E | undefined };
export type OkType<T> = { ok: true, value: T };
export type Result<T, E = undefined> = OkType<T> | ErrType<E>;

export const Ok = <T>(data: T): OkType<T> => {
    return { ok: true, value: data };
};
export const Err = <E>(error?: E): ErrType<E> => {
    return { ok: false, error };
};
export const to_res = (v: Outcome, extract: "result" | "output" = "output"): Result<string, string> => {
    if (v.error != null) return Err(v.error);
    const data = v.success!![extract];
    const dataString = typeof data !== 'string' ? JSON.stringify(data) : data; // ? return Err(`Incorrect type returned by VRL transform: '${typeof d}', expected 'string'`);
    return Ok(dataString)
}


export function readFile(stream: NodeJS.ReadableStream, isGzip: boolean) {
    if (isGzip) {
        stream = stream.pipe(zlib.createGunzip());
    }

    return readline.createInterface({
        input: stream,
        crlfDelay: Infinity,
    });
}

interface ChunkingProps<T> {
    maxLength?: number;
    maxSize?: number;
    sizeFn?: (item: T) => number;
}

export const chunkedBy = <T>(arr: T[], { maxLength, maxSize, sizeFn }: ChunkingProps<T>): T[][] => {
    if ((maxSize == null) != (sizeFn == null)) {
        throw new Error("maxSize and sizeFn must be specified together.");
    }

    let result: T[][] = [];
    let sublist: T[] = [];
    let sublistSize = 0;
    for (const item of arr) {
        let startNewArray = false;
        if (maxLength && sublist.length >= maxLength) {
            startNewArray = true;
        }
        if (maxSize && sizeFn && sublistSize + sizeFn(item) > maxSize) {
            startNewArray = true;
        }
        if (startNewArray) {
            result.push(sublist);
            sublist = [];
            sublistSize = 0;
        }
        sublist.push(item);
        if (sizeFn) sublistSize += sizeFn(item);
    }
    if (sublist.length > 0) result.push(sublist);
    return result;
};
