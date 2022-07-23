import csv, os, json, sys, random
import tarfile
import tempfile
from urllib.request import urlretrieve
from pprint import pprint
from copy import deepcopy


# This script downloads the ECS schema from github and generates an Apache Iceberg
# schema that Matano can use.

ECS_VERSION = "8.3.1"
ECS_RELEASE_URL = f"https://github.com/elastic/ecs/archive/refs/tags/v{ECS_VERSION}.tar.gz"

filename, _ = urlretrieve(ECS_RELEASE_URL) # "/home/samrose/Downloads/ecs-8.3.1.tar.gz"

extract_dir = tempfile.mkdtemp()
with tarfile.open(filename) as tf:
    tf.extractall(extract_dir)

csv_filepath = f"{extract_dir}/ecs-{ECS_VERSION}/generated/csv/fields.csv"

with open(csv_filepath) as csvf:
    reader = csv.DictReader(csvf)
    ecs_fields_raw = list(reader)

def map_ecs_iceberg_type(ecs_type):
    if ecs_type == "keyword":
        return "string"
    elif ecs_type == "scaled_float":
        return "float"
    elif ecs_type == "date":
        return "timestamp"
    elif ecs_type == "wildcard":
        return "string"
    elif ecs_type == "float":
        return "float"
    elif ecs_type == "object":
        return "string"
    elif ecs_type == "constant_keyword":
        return "string"
    elif ecs_type == "boolean":
        return "boolean"
    elif ecs_type == "long":
        return "long"
    elif ecs_type == "geo_point":
        return {
            "type": "struct",
            "fields": [
                # TODO: FIX THIS
                {
                    "id": random.randint(7000, 8000),
                    "name": "lon",
                    "required": True,
                    "type": "float",
                    "initial-default": None,
                    "write-default": None,
                },
                {
                    "id": random.randint(8000, 9000),
                    "name": "lat",
                    "required": True,
                    "type": "float",
                    "initial-default": None,
                    "write-default": None,
                }
            ]
        }
    elif ecs_type == "nested":
        return {
            "type": "list",
            "element-id": random.randint(9000, 10000),
            "element-required": False,
            "element": "string",
        }
    elif ecs_type == "match_only_text":
        return "string"
    elif ecs_type == "ip":
        return "string"
    elif ecs_type == "flattened":
        return "string"
    else:
        raise Exception(f"Unknown ECS type: {ecs_type}")

def find_arr(arr, pred):
    return next((x for x in arr if pred(x)))

def make_ecs_field_ids(ecs_fields):
    all_parts = []
    for ecs_field in ecs_fields:
        parts = ecs_field["Field"].split(".")
        for subidx, _ in enumerate(parts):
            subpart = ".".join(parts[:subidx+1])
            all_parts.append(subpart)

    return { part: idx for idx, part in enumerate(all_parts)}


ecs_field_ids = make_ecs_field_ids(ecs_fields_raw)

def ecs_field_id(col_name):
    return ecs_field_ids[col_name]


def get_field_val(dic, path):
    ret = deepcopy(dic)
    for part in path:
        access_obj = ret["type"]["fields"] if (isinstance(ret["type"], dict)) else ret["fields"]
        ret = find_arr(access_obj, lambda f: f["name"] == part)
    return ret

def get_iceberg_field_name(ecs_field_name):
    # Athena doesn't support `@` in field names
    if ecs_field_name == "@timestamp":
        return "ts"
    else:
        return ecs_field_name

def add_struct(obj, path, ecs_field, is_leaf):
    copy = obj

    colname = ".".join(path)

    for idx in range(len(path) - 1):
        part = path[idx]
        access_obj = copy["type"]["fields"] if (isinstance(copy["type"], dict)) else copy["fields"]
        copy = find_arr(access_obj, lambda f: f["name"] == part)

    append_obj = copy["type"]["fields"] if (isinstance(copy["type"], dict)) else copy["fields"]

    if is_leaf:
        append_obj.append({
            "id": ecs_field_id(colname),
            "name": get_iceberg_field_name(path[-1]),
            "type": map_ecs_iceberg_type(ecs_field["Type"]),
            "required": False,
            "doc": ecs_field["Description"],
        })
    else:
        append_obj.append({
            "id": ecs_field_id(colname),
            "name": get_iceberg_field_name(path[-1]),
            "type": {
                "type": "struct",
                "fields": [],
            },
            "required": False,
            "doc": ecs_field["Description"],
        })

def insert_col(obj, ecs_field, ecs_fields_raw):
    col_name = ecs_field["Field"]
    parts = col_name.split(".")

    for idx in range(len(parts)):
        subpath = parts[:idx+1]
        try:
            ecs_field_parent = find_arr(ecs_fields_raw, lambda f: f["Field"] == ".".join(col_name.split(".")[:-1]))
        except StopIteration:
            ecs_field_parent = None

        if (subpath[-1] == "text" and ecs_field["Type"] == "match_only_text"):
            continue
        elif (col_name.startswith("dns.answers")):
            continue
        elif ecs_field_parent and ecs_field_parent["Type"] == "nested":
            continue
        elif ecs_field_parent and ecs_field_parent["Type"] == "object":
            continue
        elif (col_name.startswith("email.attachments")):
            continue
        elif (col_name.startswith("faas.trigger")):
            continue
        elif (col_name.startswith('file.elf.sections')):
            continue
        elif (col_name.startswith('file.elf.segments')):
            continue
        elif (col_name.startswith('log.syslog')):
            continue
        elif (col_name.startswith('network.inner')):
            continue
        elif ("tty" in col_name):
            continue
        elif (col_name.startswith('observer.egress')):
            continue
        elif (col_name.startswith('observer.ingress')):
            continue
        elif (col_name.startswith('threat.enrichments')):
            continue

        try:
            get_field_val(obj, subpath)
        except StopIteration:
            add_struct(obj, subpath, ecs_field, ".".join(subpath) == col_name)


ecs_version_int = int(ECS_VERSION.replace(".", ""))

if __name__ == "__main__":
    ret = { "type": "struct", "fields": [], "schema-id": ecs_version_int, }

    for ecs_field in ecs_fields_raw:
        insert_col(ret, ecs_field, ecs_fields_raw)

    outpath = os.path.join(__file__, "../../data/ecs_iceberg_schema.json")
    with open(os.path.abspath(outpath), "w") as outf:
        json.dump(ret, outf)
