import time
from collections.abc import Iterable, MutableMapping
from typing import Union

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer


class RemoteCache(MutableMapping):
    """
    A remote cache backed by DDB.
    """

    _pk_attr_name = "pk"
    _sk_attr_name = "sk1"
    _val_attr_name = "val"
    _ttl_attr_name = "ttl"

    def __init__(self, dynamodb, table_name, namespace, ttl=3600) -> None:
        self._ddb = dynamodb
        self._table_name = table_name
        self._ttl = ttl
        self._namespace = namespace
        self._pk_attr = {"S": namespace}
        self._type_serializer = TypeSerializer()
        self._type_deserializer = TypeDeserializer()

    def __getitem__(self, __key):
        key = {"S": str(__key)}
        res = self._ddb.get_item(
            TableName=self._table_name,
            Key={
                self._pk_attr_name: self._pk_attr,
                self._sk_attr_name: key,
            },
            # ConsistentRead=True,
        )

        if "Item" not in res:
            return None

        item = res["Item"]
        value = item.get(self._val_attr_name)
        ret = self._type_deserializer.deserialize(value)
        return ret

    def __setitem__(self, __key, __value) -> None:
        ddb_value = self._type_serializer.serialize(__value)
        key = {"S": str(__key)}
        ttl_attr = self._get_ttl_attr()
        self._ddb.put_item(
            TableName=self._table_name,
            Item={
                self._pk_attr_name: self._pk_attr,
                self._sk_attr_name: key,
                self._val_attr_name: ddb_value,
                self._ttl_attr_name: ttl_attr,
            },
        )
        return None

    def __delitem__(self, key):
        ddb_key = {"S": str(key)}
        self._ddb.delete_item(
            TableName=self._table_name,
            Key={self._pk_attr_name: self._pk_attr, self._sk_attr_name: ddb_key},
        )

    def increment_counter(self, key: str, increment_value: int = 1):
        assert isinstance(
            increment_value, int
        ), "counter increment/decrement must be an integer"
        ddb_value = {"N": str(increment_value)}
        res = self._add_remove_ddb(key, ddb_value, "ADD")
        ret = int(res["N"])
        return ret

    def decrement_counter(self, key: str, decrement_value: int = 1):
        return self.increment_counter(key, decrement_value * -1)

    def add_to_string_set(self, key: str, strings: Union[Iterable[str], str]):
        return self._string_set_op(key, strings, "ADD")

    def remove_from_string_set(self, key: str, strings: Union[Iterable[str], str]):
        return self._string_set_op(key, strings, "DELETE")

    def __iter__(self):
        raise NotImplementedError()

    def __len__(self):
        raise NotImplementedError()

    def _string_set_op(self, key, strings, operation):
        if isinstance(strings, str):
            strings = [strings]
        else:
            assert isinstance(
                strings, Iterable
            ), "Provide a string or iterable to add/remove from string set"
            assert all(
                isinstance(s, str) for s in strings
            ), "Can only use strings with a string set"
            strings = list(set(strings))
        ddb_value = {"SS": strings}
        res = self._add_remove_ddb(key, ddb_value, operation)
        ret = res["SS"]
        return ret

    def _add_remove_ddb(self, key, ddb_value, operation):
        ddb_key = {"S": str(key)}
        update_expr = f"{operation} #val :value SET #ttl = if_not_exists(#ttl, :ttl)"
        res = self._ddb.update_item(
            TableName=self._table_name,
            Key={self._pk_attr_name: self._pk_attr, self._sk_attr_name: ddb_key},
            UpdateExpression=update_expr,
            ExpressionAttributeNames={
                "#val": self._val_attr_name,
                "#ttl": self._ttl_attr_name,
            },
            ExpressionAttributeValues={
                ":value": ddb_value,
                ":ttl": self._get_ttl_attr(),
            },
            ReturnValues="UPDATED_NEW",
        )
        ret = res["Attributes"][self._val_attr_name]
        return ret

    def _get_ttl_attr(self):
        ttl_val = int(time.time()) + self._ttl
        return {"N": str(ttl_val)}
