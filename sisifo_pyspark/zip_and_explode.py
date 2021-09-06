import pyspark.sql.functions as F

import sisifo
from sisifo_pyspark import utils


@sisifo.add_task("pyspark")
class ZipAndExplode(sisifo.EntityTask):
    def __init__(self, columns=[], **kwargs):
        self.columns = utils.validate_list_of_columns(columns)
        super().__init__(**kwargs)

    def run_entity(self, df):
        df = df.withColumn("zipped", F.arrays_zip(*self.columns))
        df = df.withColumn("explode", F.explode("zipped"))
        for c in self.columns:
            df = df.withColumn(c, F.col(f"explode.{c}"))
        df = df.drop("zipped", "explode")
        return df
