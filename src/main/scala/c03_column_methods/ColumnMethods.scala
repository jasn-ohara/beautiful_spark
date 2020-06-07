package c03_column_methods

import utils.ExampleRunner
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.lit

object ColumnMethods extends App with ExampleRunner  {

  import spark.implicits._


  printHeader("Using a default column method")
  val cityDf =
  Seq(
    ("thor", "new york"),
    ("aquaman", "atlantis"),
    ("wolverine", "new jersey")
  )
    .toDF("superhero","city")
    .withColumn("city_starts_with_new", $"city".startsWith("new"))
  cityDf.show()


  printHeader("Using a default column method on a predefined column")
  val cityCol = cityDf("city")
  cityDf
    .withColumn("city_starts_with_new", cityCol.startsWith("new"))
    .show()


  printHeader("Common default column methods")
  Seq(
    (10, "cat", "cat"),
    (4, "dog", "hamster"),
    (7, null, null)
  )
    .toDF("num","word", "extra_word")
    .withColumn("gt_5_a", $"num".gt(5))
    .withColumn("gt_5_b", $"num" gt 5)
    .withColumn("gt_5_c", $"num" > 5)
    .withColumn("word_first_two", $"word".substr(0, 2))
    .withColumn("pls_5", $"num" + 5)
    .withColumn("2_div", lit(2) / $"num")
    .withColumn("is_null", $"word".isNull)
    .show()


  printHeader("Creating a column method")
  def sumColumns(num1: Column, num2:Column): Column = {
    num1 + num2
  }
  Seq((10, 4), (3, 4), (8,4))
    .toDF("some_num","another_num")
    .withColumn("the_sum",sumColumns($"some_num", $"another_num"))
    .show()


  printHeader("Transforming a whole DataFrame")
  def withCat(name:String)(df: DataFrame): DataFrame = {
    df.withColumn("cat", lit(s"$name meow"))
  }
 Seq("chair", "hair", "bear").toDF("thing")
    .transform(withCat("darla"))
    .show()



}
