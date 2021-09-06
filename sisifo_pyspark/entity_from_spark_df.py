from pyspark.sql.types import _parse_datatype_string

import sisifo
from sisifo_pyspark import utils
from .pyspark_backend import PySparkBackend


def dict_to_struct_type_str(schema):
    fields = []
    for k, v in schema.items():
        if type(v) == dict:
            v = dict_to_struct_type_str(v)
        fields.append(f"{k}: {v}")
    struct = ", ".join(fields)
    struct = f"struct<{struct}>"
    return struct


def dict_to_struct_type(schema):
    struct_str = dict_to_struct_type_str(schema)
    struct = _parse_datatype_string(struct_str)
    return struct


@sisifo.add_task("pyspark")
class EntityFromSparkDF(sisifo.Task):
    def __init__(self, data=None, schema=None, entity=None, entity_out=None, **kwargs):
        super().__init__(**kwargs)

        self.entity_out = utils.validate_entity_out(entity, entity_out)

        if not data:
            data = []

        if schema and type(schema) not in [list, dict]:
            raise ValueError("Schema must be a list or a dict")

        self.data = data
        self.schema = schema
        if type(schema) == dict:
            self._schema = dict_to_struct_type(schema)
        else:
            self._schema = schema

        self._spark = PySparkBackend().get_spark()

    def run(self, data):
        # TODO: if self._schema is an AbstractTask run it here and use its output as schema.
        df = self._spark.createDataFrame(self.data, self._schema)
        data[self.entity_out] = df
