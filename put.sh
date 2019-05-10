
# upload .jar, .egg and .py files to dbfs

dst_dir=dbfs:/tmp/jobs/python-scala-udf-job

put() {
  src=$1 ; dst=$2
  echo "====="
  echo "SRC: $src" ; echo "DST: $dst"
  databricks fs cp $src $dst --overwrite
}
put_all() {
  file=spark_python_scala_udf-0.0.1-py2.7.egg  ; put python/dist/$file $dst_dir/$file
  file=spark-python-scala-udf_2.11-0.0.1-SNAPSHOT.jar ; put scala/target/scala-2.11/$file $dst_dir/$file
  file=main.py ; put python/$file $dst_dir/$file
}

put_all 
