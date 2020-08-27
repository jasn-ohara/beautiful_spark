package c11_maptype

import utils.ExampleRunner
import org.apache.spark.sql.functions.{element_at, map, map_from_arrays, map_concat}

object MapTypeExample extends App with ExampleRunner  {
  import spark.implicits._

  printHeader("Creating a MapType column:")
  val df = Seq(
    (1, "mary", Map("age" -> "23", "nationality" -> "irish")),
    (2, "raj", Map("age" -> "27", "nationality" -> "indian"))
  ).toDF("id", "name", "details")
  df.show(false)
  df.printSchema()

  printHeader("Getting the value from a map with element_at:")
  df
    .withColumn("nationality", element_at($"details", "nationality"))
    .show(false)


  printHeader("Appending a MapType column:")
  val secondMapDf = df.withColumn("id_name", map($"id", $"name"))
  secondMapDf.show()

  printHeader("Creating a MapType from two ArrayTypes:")
  val dfWithArrays = Seq(
    ("C1", Array("A32","C32"),Array(20,40)),
    ("C2", Array("B65","A12"),Array(18,87)),
    ("C3", Array("F65","A12"),Array(28,90))
  ).toDF("customer", "purchase_cds", "purchase_amnts")

  // This is quite different from the normal Scala style of using a zipping function
  dfWithArrays
    .withColumn("purchaces", map_from_arrays($"purchase_cds", $"purchase_amnts"))
    .show(false)

  printHeader("Merging Maps with map_concat:")
  secondMapDf
    .withColumn("all_data", map_concat($"id_name", $"details"))
    .show()
}
