
JAR=scala/target/scala-2.11/spark-python-scala-udf_2.11-0.0.1-SNAPSHOT.jar
EGG=python/dist/spark_python_scala_udf-0.0.1-py2.7.egg
spark-submit --master local[2] --jars $JAR --py-files $EGG python/main.py 
