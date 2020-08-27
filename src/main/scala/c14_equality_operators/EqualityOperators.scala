package c14_equality_operators

import utils.ExampleRunner

import java.sql.Date
import java.sql.Timestamp
import org.apache.spark.sql.functions._

object EqualityOperators extends App with ExampleRunner  {
  import spark.implicits._

  printHeader("Creating a DateType Column")
  val dateTypeDf = Seq(
    (1, Date.valueOf("1993-02-04")),
    (2, Date.valueOf("1996-02-09"))
  ).toDF("id", "birth_date")
  dateTypeDf.show()
  dateTypeDf.printSchema()

  printHeader("Creating a DateType Column")
  val castDateTypeDf = Seq(
    (1, "1993-02-04"),
    (2, "1996-02-09")
  )
    .toDF("id", "birth_date")
    .withColumn("birth_date", $"birth_date" cast "date")
  castDateTypeDf.show()
  castDateTypeDf.printSchema()

  printHeader("Getting the year, month and day from a DateType")
  dateTypeDf
    .withColumn("birth_year", year($"birth_date"))
    .withColumn("birth_month", month($"birth_date"))
    .withColumn("birth_day", dayofmonth($"birth_date"))
    .show()

  printHeader("Date addition and subtraction")
  dateTypeDf
    .withColumn("diff_in_days", datediff(current_timestamp(), $"birth_date"))
    .withColumn("add_15_days", date_add($"birth_date", 15))
    .show()

  printHeader("Creating a TimestampType Column")
  val tsDf = Seq(
    (1, Timestamp.valueOf("2017-12-02 03:04:00")),
    (2, Timestamp.valueOf("2018-11-03 21:04:00")),
    (3, Timestamp.valueOf("1993-12-02 08:53:00")),
    (4, Timestamp.valueOf("2005-12-02 03:04:00"))
  ).toDF("Id", "time")
  tsDf.show()

  printHeader("Getting the hour, minute and second from a TimestampType")
  tsDf
    .withColumn("hour", hour($"time"))
    .withColumn("min", minute($"time"))
    .withColumn("sec", second($"time"))
    .show()

}
