package org.andre.udf

import org.apache.spark.sql.SQLContext

object Functions {
  def cube(n: Int) = n * n * n

  def registerFunc(sqlContext: SQLContext, name: String) {
    val f = cube(_)
    sqlContext.udf.register(name, f)
  }
}
