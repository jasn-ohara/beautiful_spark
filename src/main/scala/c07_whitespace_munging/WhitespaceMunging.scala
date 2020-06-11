package c07_whitespace_munging

import utils.ExampleRunner
import org.apache.spark.sql.functions.{trim, ltrim, rtrim}
import com.github.mrpowers.spark.daria.sql.functions.{removeAllWhitespace, singleSpace}

object WhitespaceMunging extends App with ExampleRunner  {

  import spark.implicits._

  printHeader("The various functions for manipulating whitespace")
  Seq(
    "    i like cheese  ",
    "the  dog runs   ",
    null
  )
    .toDF("words")
    .withColumn("trim", trim($"words"))
    .withColumn("ltrim", ltrim($"words"))
    .withColumn("rtrim", rtrim($"words"))
    .withColumn("single_spce", singleSpace($"words"))
    .withColumn("no_whitespace", removeAllWhitespace($"words"))
    .show
}
