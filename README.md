# Spark Python Scala UDF

Demonstrates calling a Scala UDF from Python using spark-submit with an EGG and JAR.

Prerequities:
* Spark 2.3.0
* Python 2.7


## Code

### [main.py](python/main.py)
```
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
```

### [functions.py](python/udf/functions.py)
```
import sys

def square(s):
  return s * s

def do_sql(spark,sqlContext):
  print "Calling Python UDF with SQL"
  sqlContext.udf.register("squareWithPython", square)
  spark.sql("select id, squareWithPython(id) as id_square_sql from test").show()

def do_sql_scala(spark,sqlContext):
  print "Calling Scala UDF with SQL"
  spark._jvm.org.andre.udf.Functions.registerFunc(sqlContext._jsqlContext,"cube")
  spark.sql("select id, cube(id) as id_cube_scala from test").show()

def do_df(spark,sqlContext):
  from pyspark.sql.functions import udf
  from pyspark.sql.types import LongType
  print "Calling Python UDF with DataFrame"
  square_udf = udf(square, LongType())
  df = sqlContext.table("test")
  df.select("id", square_udf("id").alias("id_square_df")).show()
```

### [Functions.scala](scala/src/main/scala/org/andre/udf/Functions.scala)
```
package org.andre.udf
import org.apache.spark.sql.SQLContext

object Functions {
  def cube(n: Int) = n * n * n

  def registerFunc(sqlContext: SQLContext, name: String) {
    val f = cube(_)
    sqlContext.udf.register(name, f)
  }
}
```

## Build and run

[build.sh](build.sh)
```
cd python
python setup.py bdist_egg
cd ../scala
sbt clean package
```

[run.sh](run.sh)
```
JAR=scala/target/scala-2.11/spark-python-scala-udf_2.11-0.0.1-SNAPSHOT.jar
EGG=python/dist/spark_python_scala_udf-0.0.1-py2.7.egg
spark-submit --master local[2] --jars $JAR --py-files $EGG python/main.py 
```

```
Calling Python UDF with SQL
+---+-------------+
|id |id_square_sql|
+---+-------------+
|1  |1            |
|2  |4            |
|3  |9            |
|4  |16           |
+---+-------------+

Calling Python UDF with DataFrame
+---+------------+
|id |id_square_df|
+---+------------+
|1  |1           |
|2  |4           |
|3  |9           |
|4  |16          |
+---+------------+

Calling Scala UDF with SQL
+---+-----------+
|id |id_cube_jvm|
+---+-----------+
|1  |1          |
|2  |8          |
|3  |27         |
|4  |64         |
+---+-----------+
```
