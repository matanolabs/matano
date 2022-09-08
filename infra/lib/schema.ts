import * as fs from "fs";
import * as path from "path";
import { dataDirPath } from "./utils";

const DEFAULT_ECS_FIELD_NAMES: string[] = ["ts", "labels", "tags"];

export function resolveSchema(ecsFieldNames?: string[], customFields?: any[]) {
    const baseSchema = JSON.parse(fs.readFileSync(path.join(dataDirPath, "ecs_iceberg_schema.json")).toString());

    const ecsFields = [...DEFAULT_ECS_FIELD_NAMES, ...(ecsFieldNames ?? [])];
    const customFieldsResolved = customFields ?? [];
    const retFields: any[] = baseSchema["fields"].filter((f: any) => ecsFields.includes(f["name"]));

    for (const customField of customFieldsResolved) {
        if (ecsFields.includes(customField["name"])) {
            throw new Error("Custom field overriding ECS field.");
        }
        retFields.push(customField);
    }

    return { ...baseSchema, fields: retFields }
}
