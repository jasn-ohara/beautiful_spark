package c12_structtype

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import utils.ExampleRunner

object StructTypeExample extends App with ExampleRunner  {
  import spark.implicits._

  printHeader("Creating a flat schema with StructType:")
  val data = Seq(
    Row(1, "a"),
    Row(5, "b"),
    Row(10, "z")
  )
  val schema = StructType(
    List(
      StructField("num", IntegerType, nullable = true),
      StructField("letter", StringType, nullable = true)
    )
  )
  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(data),
    schema
  )

  df.show()
  df.printSchema()

  printHeader("Adding a nested StructType:")
  val nested =
    df
      .withColumn("details",
        struct(
          $"num" < 5 as "<5",
          $"num" % 2 === 0 as "even"
        )
      )
  nested.show()
  nested.printSchema()

  printHeader("Flattening a nested StructType:")
  val flattened =
    nested
      .select(
        $"num",
        $"letter",
        $"details"("<5") as "<5",
        $"details"("even") as "even"
      )
  flattened.show()
  flattened.printSchema()


  printHeader("Using StructType to nest dependent code: ")
  val df2 = Seq(
    (14, "happy"),
    (32, "sad"),
    (10, "glad"),
    (16, "happy"),
    (13, "sad")
  ).toDF("age", "mood")

  val isTeenager = $"age" between (13, 19)
  val hasPositiveMood = $"mood" isin ("happy", "glad")
  val nestedStructDf =
    df2.withColumn(
      "best_action",
      struct(
        isTeenager as "is_teenager",
        hasPositiveMood as "has_positive_mood",
        when(
          isTeenager && hasPositiveMood,
          "have a chat"
        ) as "what_to_do"
      )
    )
  nestedStructDf.show()
  nestedStructDf.printSchema()


}
