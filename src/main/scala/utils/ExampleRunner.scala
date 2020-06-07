package utils

import org.apache.spark.sql.SparkSession

trait ExampleRunner {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("beautiful_spark")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  def printHeader(header: String): Unit = {
    println("\n==================================================")
    println(header)
    println("==================================================")
  }
}
