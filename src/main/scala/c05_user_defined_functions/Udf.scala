package c05_user_defined_functions

import org.apache.spark.sql.{Column, DataFrame}
import utils.ExampleRunner
import org.apache.spark.sql.functions.{udf, lower, regexp_replace}

object Udf extends App with ExampleRunner  {

  import spark.implicits._

  printHeader("A basic udf")
  def lowerRemoveAllWhitespace(s: String): String = {
    s.toLowerCase.replaceAll("\\s", "")
  }
  val lowerRemoveAllWhitespaceUdf = udf[String, String](lowerRemoveAllWhitespace)
  val df: DataFrame = Seq(
    "     HI THERE     ",
    "   GivE mE PresenTS     "
  )
    .toDF("wtf")
  df
    .select(lowerRemoveAllWhitespaceUdf($"wtf") as "clean_wtf")
    .show()


  printHeader("A udf that handles null")
  def betterLowerRemoveAllWhitespace(s: String): Option[String] = {
    val str = Option(s).getOrElse(return None)
    Some(str.toLowerCase().replaceAll("\\s", ""))
  }
  val betterLowerRemoveAllWhitespaceUdf = udf[Option[String], String](betterLowerRemoveAllWhitespace)
  val nullDf: DataFrame = Seq("blabla", null)
    .toDF("wtf")
    .union(df)
  nullDf
    .select(betterLowerRemoveAllWhitespaceUdf($"wtf") as "clean_wtf")
    .show


  printHeader("Replacing udf with regular sql function")
  def bestLowerRemoveAllWhitespace(col: Column): Column = {
    lower(regexp_replace(col, "\\s+", ""))
  }
  nullDf
    .select(bestLowerRemoveAllWhitespace($"wtf") as "clean_wtf")
    .show()

}
