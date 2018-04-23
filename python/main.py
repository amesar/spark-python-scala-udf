import sys
from udf import functions
from pyspark.sql import SparkSession,SQLContext

if __name__ == "__main__":
  spark = SparkSession.builder.appName("PythonScalaUDF").getOrCreate()
  sc = spark.sparkContext
  sqlContext = SQLContext(spark.sparkContext)
  sqlContext.range(1, 4).registerTempTable("test")
  functions.do_sql(spark,sqlContext)
  functions.do_df(spark,sqlContext)
  functions.do_sql_scala(spark,sqlContext)
