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

function deepen(obj: any) {
  const result: any = {};

  // For each object path (property key) in the object
  for (const objectPath in obj) {
    // Split path into component parts
    const parts = objectPath.split(".");

    // Create sub-objects along path as needed
    let target = result;
    while (parts.length > 1) {
      const part = parts.shift()!;
      target = target[part] = target[part] || {};
    }

    // Set value at end of path
    target[parts[0]] = obj[objectPath];
  }

  return result;
}

export function merge(a: any, b: any, inputPath: string[] | undefined = undefined) {
  /** merges b into a */
  const path: string[] = inputPath || [];

  for (const key in b) {
    if (a.hasOwnProperty(key)) {
      if (isObject(a[key]) && isObject(b[key])) {
        merge(a[key], b[key], [...path, key]);
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

const deepGet = (obj: any, keys: string | (string | number)[], delimiter = ".") => {
  if (isString(keys)) {
    keys = keys.split(delimiter);
  }
  return keys.reduce((xs, x) => (xs && xs[x] ? xs[x] : null), obj);
};

const DEFAULT_ECS_FIELD_NAMES: string[] = ["ts", "labels", "tags"];

export function fieldsToSchema(fields: string[]) {
  const reducer = (acc: any, item: any) => {
    acc[item.name] = isObject(item.type)
      ? item.type.type == "struct"
        ? fieldsToSchema(item.type.fields)
        : item.type.type == "list"
        ? [item["type"]["element"]]
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
      ? { type: "list", element: v[0] }
      : v,
  }));
}

export function resolveSchema(ecsFieldNames?: string[], customFields?: any[]) {
  const baseEcsSchema = fieldsToSchema(
    JSON.parse(fs.readFileSync(path.join(dataDirPath, "ecs_iceberg_schema.json")).toString())["fields"]
  );

  const allEcsFieldNames = [...DEFAULT_ECS_FIELD_NAMES, ...(ecsFieldNames ?? [])];

  const relevantEcsSchema = allEcsFieldNames.reduce((acc, fieldName) => {
    let field = deepGet(baseEcsSchema, fieldName);
    if (field == null) {
      throw new Error(`Field ${fieldName} not found in ECS schema`);
    }
    return merge(acc, deepen({ [fieldName]: field }));
  }, {} as Record<string, any>);

  let customSchema = fieldsToSchema(customFields ?? []);
  const customFieldNames = getDottedFieldPaths(customFields ?? []);
  for (const customFieldName of customFieldNames) {
    if (deepGet(baseEcsSchema, customFieldName) != null) {
      throw new Error(`Custom field conflicts with ECS field: ${customFieldName}`);
    }
  }
  customSchema = merge(customSchema, relevantEcsSchema);

  return { type: "struct", fields: serializeToFields(customSchema) };
}
