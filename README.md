# Spark Python Scala UDF

Demonstrates calling a Spark Scala UDF from Python with an EGG and a JAR.
* Using spark-submit.
* Using Databricks REST API endpoint [jobs/runs/submit](https://docs.databricks.com/api/latest/jobs.html#runs-submit).

Prerequities:
* Spark 2.4.2
* Python 2.7
* Scala 2.11.8
* curl

## Code

### [main.py](python/main.py)
```
import sys
from my_udf.functions import square
from pyspark.sql import SparkSession,SQLContext

spark = SparkSession.builder.appName("PythonScalaUDF").getOrCreate()
spark.range(1, 4).createOrReplaceTempView("test")

def call_python_udf_sql():
  print("Calling Python UDF with SQL")
  spark.udf.register("squareWithPython", square)
  spark.sql("select id, squareWithPython(id) as id_square_sql from test").show()

def call_python_udf_df():
  from pyspark.sql.functions import udf
  from pyspark.sql.types import LongType
  print("Calling Python UDF with DataFrame")
  square_udf = udf(square, LongType())
  df = spark.table("test")
  df.select("id", square_udf("id").alias("id_square_df")).show()

def call_scala_udf_sql():
  print("Calling Scala UDF with SQL")
  sqlContext = SQLContext(spark.sparkContext)
  spark._jvm.com.databricks.solutions.udf.Functions.registerFunc(sqlContext._jsqlContext,"cube")
  spark.sql("select id, cube(id) as id_cube_sql_scala from test").show()

if __name__ == "__main__":
  call_python_udf_sql()
  call_python_udf_df()
  call_scala_udf_sql()
```

### [functions.py](python/my_udf/functions.py)
```
import sys

def square(s):
  return s * s

```

### [Functions.scala](scala/src/main/scala/com/databricks/solutions/udf/Functions.scala)
```
package com.databricks.solutions.udf
import org.apache.spark.sql.SQLContext

object Functions {
  def cube(n: Int) = n * n * n

  def registerFunc(sqlContext: SQLContext, name: String) {
    val f = cube(_)
    sqlContext.udf.register(name, f)
  }
}
```

## Build

[build.sh](build.sh)
```
cd python
python setup.py bdist_egg
cd ../scala
sbt clean package
```

## Run

### Run with spark-submit
[spark-submit.sh](spark-submit.sh)
```
JAR=scala/target/scala-2.11/spark-python-scala-udf_2.11-0.0.1-SNAPSHOT.jar
EGG=python/dist/spark_python_scala_udf-0.0.1-py2.7.egg
spark-submit --master local[2] --jars $JAR --py-files $EGG python/main.py 
```

### Run with Databricks REST API endpoint jobs/runs/submit

Steps:
* Set your API URL and token in [setup.env](setup.env).
* Create the sample DBFS job path /tmp/python-scala-udf-job with [mkdir.sh](mkdir.sh).
* Upload the JAR, EGG and Python main files to dbfs with [put.sh](put.sh) to above path.
* Tweak your job request file in [run_submit.json](run_submit.json).
* Submit the job with [run_submit.sh](run_submit.sh).

### Run output
```
Calling Python UDF with SQL
+---+-------------+
| id|id_square_sql|
+---+-------------+
|  1|            1|
|  2|            4|
|  3|            9|
+---+-------------+

Calling Python UDF with DataFrame
+---+------------+
| id|id_square_df|
+---+------------+
|  1|           1|
|  2|           4|
|  3|           9|
+---+------------+

Calling Scala UDF with SQL
+---+-----------------+
| id|id_cube_sql_scala|
+---+-----------------+
|  1|                1|
|  2|                8|
|  3|               27|
+---+-----------------+
```
