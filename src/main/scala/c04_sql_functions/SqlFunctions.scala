package c04_sql_functions

import utils.ExampleRunner

import org.apache.spark.sql.functions.{factorial, lit, when}
import org.apache.spark.sql.Column


object SqlFunctions extends App with ExampleRunner  {

  import spark.implicits._


  printHeader("Some of the many functions available in org.apache.spark.sql.functions")
  val ageDf =
  Seq(
    ("Jim", 12),
    ("Mary", 20),
    ("Pat", 18),
    ("Alice", 16),
    ("John", 4)
  ).toDF("name", "age")
  ageDf
    .withColumn("age_fact", factorial($"age"))
    .withColumn("is_person", lit(true))
    .withColumn("is_adult", when($"age" >= 18, true).otherwise(false))
    .show()


  printHeader("A custom SQL function")
  def lifeStage(col: Column): Column = {
    when(col < 13, "child")
      .when(col >= 13 && col <= 18, "teenager")
      .when(col > 18, "adult")
  }
  ageDf
    .withColumn("life_stage", lifeStage($"age"))
    .show()
}
