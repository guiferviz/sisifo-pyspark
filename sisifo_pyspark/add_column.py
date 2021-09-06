import pyspark.sql.functions as F

import sisifo
from sisifo_pyspark import utils


@sisifo.add_task("pyspark")
class AddColumn(sisifo.EntityTask):
    def __init__(self, column=None, column_out=None, dtype=None, default=None, **kwargs):
        super().__init__(**kwargs)
        self.column_out = utils.validate_column_out(column, column_out)
        self.dtype = dtype
        self.default = default

    def run_entity(self, df):
        value = F.lit(self.default)
        if self.dtype:
            value = value.cast(self.dtype)
        return df.withColumn(self.column_out, value)
