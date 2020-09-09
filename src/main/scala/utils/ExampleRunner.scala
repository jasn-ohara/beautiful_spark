package utils

import java.io.File

import org.apache.spark.sql.SparkSession

trait ExampleRunner {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("beautiful_spark")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val outputLocation = "src/test/data/"

  def printHeader(header: String): Unit = {
    println("\n==================================================")
    println(header)
    println("==================================================")
  }

  def printDirContents(dir: String): Unit = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.map(_.getName).toList.foreach(println)
    }
  }
}
