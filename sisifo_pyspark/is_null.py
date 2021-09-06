import sisifo


@sisifo.add_task("pyspark")
class IsNull(sisifo.EntityColumnTask):
    def run_entity(self, df):
        return df.withColumn(self.column_out, df[self.column_in].isNull())
