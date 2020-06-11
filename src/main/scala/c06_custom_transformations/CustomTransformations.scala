package c06_custom_transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import utils.ExampleRunner

object CustomTransformations extends App with ExampleRunner  {

  import spark.implicits._

  printHeader("Chaining custom DataFrame transformations")
  def withGreeting(df: DataFrame): DataFrame = {
    df.withColumn("greeting", lit("hello"))
  }
  def withFarewell(df: DataFrame): DataFrame = {
    df.withColumn("farewell", lit("goodbye"))
  }
  Seq("funny", "person")
    .toDF("something")
    .transform(withGreeting)
    .transform(withFarewell)
    .show()

  printHeader("Chaining custom DataFrame transformations with additional arguments")
  def withCat(name: String)(df: DataFrame): DataFrame = {
    df.withColumn("cats", lit(s"$name meow"))
  }
  Seq("funny", "person")
    .toDF("something")
    .transform(withGreeting)
    .transform(withCat("puffy"))
    .show()
}
