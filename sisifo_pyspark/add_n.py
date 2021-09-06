import sisifo


@sisifo.add_task("pyspark")
class AddN(sisifo.EntityColumnTask):
    def __init__(self, n=1, **kwargs):
        super().__init__(**kwargs)
        self.n = n

    def run_entity(self, df):
        return df.withColumn(self.column_out, df[self.column_in] + self.n)
