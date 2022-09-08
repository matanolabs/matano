package com.matano.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;

/** Like {@link org.apache.iceberg.SchemaParser }, but doesn't require `id` or `required` for fields. */
public class RelaxedIcebergSchemaParser {

    private RelaxedIcebergSchemaParser() {
    }

    private static final String SCHEMA_ID = "schema-id";
    private static final String IDENTIFIER_FIELD_IDS = "identifier-field-ids";
    private static final String TYPE = "type";
    private static final String STRUCT = "struct";
    private static final String LIST = "list";
    private static final String MAP = "map";
    private static final String FIELDS = "fields";
    private static final String ELEMENT = "element";
    private static final String KEY = "key";
    private static final String VALUE = "value";
    private static final String DOC = "doc";
    private static final String NAME = "name";
    private static final String ID = "id";
    private static final String ELEMENT_ID = "element-id";
    private static final String KEY_ID = "key-id";
    private static final String VALUE_ID = "value-id";
    private static final String REQUIRED = "required";
    private static final String ELEMENT_REQUIRED = "element-required";
    private static final String VALUE_REQUIRED = "value-required";
    public static final int CUSTOM_FIELD_START_IDX = 47000;
    private static final AtomicInteger counter = new AtomicInteger(CUSTOM_FIELD_START_IDX);

    private static int getId() {
        return counter.incrementAndGet();
    }

    private static int getIntFieldOrDefault(String property, JsonNode node) {
        Integer maybeId = JsonUtil.getIntOrNull(property, node);
        return maybeId == null ? getId() : maybeId;
    }

    private static boolean getBoolDefault(String property, JsonNode node) {
        if (!node.has(property)) {
            return false;
        }
        JsonNode pNode = node.get(property);
        if (pNode == null || pNode.isNull()) {
            return false;
        }
        return pNode.asBoolean(false);
    }

    private static Type typeFromJson(JsonNode json) {
        if (json.isTextual()) {
            return Types.fromPrimitiveString(json.asText());

        } else if (json.isObject()) {
            String type = json.get(TYPE).asText();
            if (STRUCT.equals(type)) {
                return structFromJson(json);
            } else if (LIST.equals(type)) {
                return listFromJson(json);
            } else if (MAP.equals(type)) {
                return mapFromJson(json);
            }
        }

        throw new IllegalArgumentException("Cannot parse type from json: " + json);
    }

    private static Types.StructType structFromJson(JsonNode json) {
        JsonNode fieldArray = json.get(FIELDS);
        Preconditions.checkArgument(fieldArray.isArray(),
                "Cannot parse struct fields from non-array: %s", fieldArray);

        List<Types.NestedField> fields = Lists.newArrayListWithExpectedSize(fieldArray.size());
        Iterator<JsonNode> iterator = fieldArray.elements();
        while (iterator.hasNext()) {
            JsonNode field = iterator.next();
            Preconditions.checkArgument(field.isObject(),
                    "Cannot parse struct field from non-object: %s", field);

            int id = getIntFieldOrDefault(ID, field);
            String name = JsonUtil.getString(NAME, field);
            Type type = typeFromJson(field.get(TYPE));

            String doc = JsonUtil.getStringOrNull(DOC, field);
            boolean isRequired = getBoolDefault(REQUIRED, field);
            if (isRequired) {
                fields.add(Types.NestedField.required(id, name, type, doc));
            } else {
                fields.add(Types.NestedField.optional(id, name, type, doc));
            }
        }

        return Types.StructType.of(fields);
    }


    private static Types.ListType listFromJson(JsonNode json) {
        int elementId = getIntFieldOrDefault(ELEMENT_ID, json);
        Type elementType = typeFromJson(json.get(ELEMENT));
        boolean isRequired = getBoolDefault(ELEMENT_REQUIRED, json);

        if (isRequired) {
            return Types.ListType.ofRequired(elementId, elementType);
        } else {
            return Types.ListType.ofOptional(elementId, elementType);
        }
    }

    private static Types.MapType mapFromJson(JsonNode json) {
        int keyId = getIntFieldOrDefault(KEY_ID, json);
        Type keyType = typeFromJson(json.get(KEY));

        int valueId = getIntFieldOrDefault(VALUE_ID, json);
        Type valueType = typeFromJson(json.get(VALUE));

        boolean isRequired = getBoolDefault(VALUE_REQUIRED, json);

        if (isRequired) {
            return Types.MapType.ofRequired(keyId, valueId, keyType, valueType);
        } else {
            return Types.MapType.ofOptional(keyId, valueId, keyType, valueType);
        }
    }

    public static Schema fromJson(JsonNode json) {
        Type type = typeFromJson(json);
        Preconditions.checkArgument(type.isNestedType() && type.asNestedType().isStructType(),
                "Cannot create schema, not a struct type: %s", type);
        Integer schemaId = JsonUtil.getIntOrNull(SCHEMA_ID, json);
        Set<Integer> identifierFieldIds = JsonUtil.getIntegerSetOrNull(IDENTIFIER_FIELD_IDS, json);

        if (schemaId == null) {
            return new Schema(type.asNestedType().asStructType().fields(), identifierFieldIds);
        } else {
            return new Schema(schemaId, type.asNestedType().asStructType().fields(), identifierFieldIds);
        }
    }

    public static Schema fromJson(String json) {
        try {
            return fromJson(JsonUtil.mapper().readValue(json, JsonNode.class));
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }
}

