import * as fs from "fs";
import * as path from "path";
import { dataDirPath } from "./utils";

// Guard
function isString<T = any>(str: string | T): str is string {
  return typeof str === "string";
}

function isObject(v: any): boolean {
  return typeof v === "object" && !Array.isArray(v) && v !== null;
}

function isNumeric(s: string) {
  return /^\d+$/.test(s);
}

function zip<A, B>(a: A[], b: B[]): [A, B][] {
  return a.map((k, i) => [k, b[i]]);
}

function deepen(j: any) {
  const d: any = {};
  for (let [key, value] of Object.entries(j)) {
    let s = d;
    const tokens: any[] = key.match(/\w+/g) ?? [];
    for (let [zeroedIndex, [token, nextToken]] of zip(tokens, [...tokens.slice(1), value]).entries()) {
      const index = zeroedIndex + 1;
      value = index == tokens.length ? nextToken : isNumeric(nextToken) ? [] : {};
      if (Array.isArray(s)) {
        token = parseInt(token);
        while (token >= s.length) {
          s.push(value);
        }
      } else if (!(token in s)) {
        s[token] = value;
      }
      s = s[token];
    }
  }
  return d;
}

export function mergeSchema(a: any, b: any, inputPath: string[] | undefined = undefined) {
  /** mergeSchemas b into a */
  const path: string[] = inputPath || [];

  for (const key in b) {
    if (a.hasOwnProperty(key)) {
      if (isObject(a[key]) && isObject(b[key])) {
        mergeSchema(a[key], b[key], [...path, key]);
      } else if (Array.isArray(a[key]) && Array.isArray(b[key])) {
        if (a[key].length != b[key].length) {
          throw new Error(`Cannot merge schemas at ${path.join(".")}.${key}: array lengths differ`);
        }
        for (const [i, [aItem, bItem]] of zip<any, any>(a[key], b[key]).entries()) {
          a[key][i] = mergeSchema(aItem, bItem, [...path, `${key}`]);
        }
      } else if (a[key] == b[key]) {
        // do nothing
      } else {
        throw new Error(`Conflict at ${[...path, key].join(".")}`);
      }
    } else {
      a[key] = b[key];
    }
  }

  return a;
}

const deepFind = (obj: any, keys: string | (string | number)[], delimiter = ".") => {
  if (isString(keys)) {
    keys = keys.split(delimiter);
  }

  return keys.reduce(
    ({ path, value }, x) => {
      if (value == null) return { value: undefined };
      if (Array.isArray(value)) {
        path += "[0]";
        value = value[0];
      }
      value = value[x];
      return { path: value ? (path ? `${path}.${x}` : `${x}`) : undefined, value };
    },
    { path: "", value: obj } as {
      path?: string;
      value: any;
    }
  );
};

const DEFAULT_TS_FIELD_NAMES: string[] = ["ts"];
const DEFAULT_ECS_FIELD_NAMES: string[] = ["labels", "tags"];

export function fieldsToSchema(fields: any[]) {
  const reducer = (acc: any, item: any) => {
    acc[item.name] = isObject(item.type)
      ? item.type.type == "struct"
        ? fieldsToSchema(item.type.fields)
        : item.type.type == "list"
        ? [fieldsToSchema([{ name: "$element", type: item["type"]["element"] }])["$element"]]
        : "UNKNOWN"
      : item.type;
    return acc;
  };

  return fields.reduce(reducer, {});
}

function getDottedFieldPaths(fields: string[]): string[] {
  const reducer = (acc: any, item: any) => {
    if (isObject(item.type)) {
      if (item.type.type == "struct") {
        getDottedFieldPaths(item.type.fields).forEach((subItem: any) => {
          acc.push(`${item.name}.${subItem}`);
        });
      } else if (item.type.type == "list") {
        acc.push(item.name);
      } else {
        throw new Error(`Unknown type ${item.type.type}`);
      }
    } else {
      acc.push(item.name);
    }
    return acc;
  };

  return fields.reduce(reducer, []);
}

export function serializeToFields(schema: Record<string, any>): any[] {
  return Object.entries(schema).map(([k, v]: any) => ({
    name: k,
    type: isObject(v)
      ? { type: "struct", fields: serializeToFields(v) }
      : Array.isArray(v)
      ? { type: "list", element: serializeToFields({ $element: v[0] })[0]["type"] }
      : v,
  }));
}

export function resolveSchema(
  ecsFieldNames?: string[],
  customFields?: any[],
  noDefaultEcsFields = false,
  noDefaultTs = false
) {
  const baseEcsSchema = fieldsToSchema(
    JSON.parse(fs.readFileSync(path.join(dataDirPath, "ecs_iceberg_schema.json")).toString())["fields"]
  );

  const allEcsFieldNames = [];
  if (!noDefaultEcsFields) {
    allEcsFieldNames.push(...DEFAULT_ECS_FIELD_NAMES);
  }
  if (!noDefaultTs) {
    allEcsFieldNames.push(...DEFAULT_TS_FIELD_NAMES);
  }
  allEcsFieldNames.push(...(ecsFieldNames ?? []));

  const relevantEcsSchema = allEcsFieldNames.reduce((acc, fieldName) => {
    let { path, value: field } = deepFind(baseEcsSchema, fieldName);
    if (path == null) {
      throw new Error(`Field ${fieldName} not found in ECS schema`);
    }
    return mergeSchema(acc, deepen({ [path]: field }));
  }, {} as Record<string, any>);

  let customSchema = fieldsToSchema(customFields ?? []);
  const customFieldNames = getDottedFieldPaths(customFields ?? []);
  for (const customFieldName of customFieldNames) {
    let { path } = deepFind(baseEcsSchema, customFieldName);
    if (path != null) {
      throw new Error(`Custom field conflicts with ECS field: ${path}`);
    }
  }
  customSchema = mergeSchema(customSchema, relevantEcsSchema);

  return { type: "struct", fields: serializeToFields(customSchema) };
}
