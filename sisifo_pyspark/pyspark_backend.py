from sisifo_pyspark.utils import Singleton


class PySparkBackend(metaclass=Singleton):
    def set_spark(self, spark):
        self.spark = spark

    def get_spark(self):
        return self.spark
