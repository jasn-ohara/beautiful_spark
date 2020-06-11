package c09_null

import utils.ExampleRunner
import org.apache.spark.sql.functions.{udf, when, lit}

object Null extends App with ExampleRunner  {

  import spark.implicits._
  val testDf =
    spark.sparkContext.parallelize(
      Seq(new Integer(2), new Integer(3), new Integer(4), null.asInstanceOf[Integer])
    ).toDF("n")

  printHeader("Working around a bad UDF that does not handle null")
  def isEvenSimple(n: Integer): Boolean = {
    n % 2 == 0
  }
  val isEvenSimpleUdf = udf[Boolean, Integer](isEvenSimple)
  testDf
    .withColumn("is_even", when($"n".isNotNull, isEvenSimpleUdf($"n")).otherwise(lit(null)))
    .show()

  printHeader("A UDF with crude null handling")
  def isEvenBad(n: Integer): Boolean = {
    n != null && n % 2 == 0
  }
  val isEvenBadUdf = udf[Boolean, Integer](isEvenBad)
  testDf
    .withColumn("is_even", isEvenBadUdf($"n"))
    .show()

  printHeader("Using monads to deal with null")
  def isEvenBetter(n: Integer): Option[Boolean] = {
    if (n == null){
      None
    } else {
      Some(n % 2 == 0)
    }
  }
  val isEvenBetterUdf = udf[Option[Boolean], Integer](isEvenBetter)
  testDf
    .withColumn("is_even", isEvenBetterUdf($"n"))
    .show()
}
