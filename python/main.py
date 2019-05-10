import sys
from pyspark.sql import SparkSession,SQLContext

spark = SparkSession.builder.appName("PythonScalaUDF").getOrCreate()
spark.range(1, 4).createOrReplaceTempView("test")

def call_python_udf_sql():
  from my_udf.functions import square
  print("\nCalling Python UDF with SQL")
  spark.udf.register("squareWithPython", square)
  spark.sql("select id, squareWithPython(id) as square_sql from test").show()

def call_python_udf_df():
  print("Calling Python UDF with DataFrame")
  from pyspark.sql.functions import udf
  from my_udf.functions import square
  square_udf = udf(square)
  # or more type-safe
  # from pyspark.sql.types import LongType
  # square_udf = udf(square, LongType())
  df = spark.table("test")
  df.select("id", square_udf("id").alias("square_df")).show()

def call_scala_udf_sql():
  print("Calling Scala UDF with SQL")
  sqlContext = SQLContext(spark.sparkContext)
  spark._jvm.com.databricks.solutions.udf.Functions.registerFunc(sqlContext._jsqlContext,"cube")
  spark.sql("select id, cube(id) as cube_sql_scala from test").show()

if __name__ == "__main__":
  call_python_udf_sql()
  call_python_udf_df()
  call_scala_udf_sql()
