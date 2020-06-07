package c02_csv

import org.apache.spark.sql.SaveMode
import utils.ExampleRunner
import org.apache.spark.sql.functions.lit

object Csv extends App with ExampleRunner  {


  printHeader("Reading DataFrame from CSV")
  val csvInputPath = "src/main/resources/example_input.csv"
  val df = spark.read.option("header","true").csv(csvInputPath)
  df.show()
  df.printSchema()


  printHeader("Writing DataFrame from CSV")
  val csvOutputPath = "src/main/resources/example_output.csv"
  df
    .withColumn("flag", lit("flagged"))
    .write
    .mode(SaveMode.Overwrite)
    .csv(csvOutputPath)
  val wroteDf = spark.read.csv(csvOutputPath)
  wroteDf.show()
  wroteDf.printSchema()
}
