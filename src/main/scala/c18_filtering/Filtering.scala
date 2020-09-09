package c18_filtering

import utils.ExampleRunner
object Filtering extends App with ExampleRunner  {

  import spark.implicits._

  printHeader("Viewing a read with no partition filters")
  val outputPathSinglePartition = outputLocation + "partitioning/singlePartition"
  spark.read.parquet(outputPathSinglePartition)
    .where($"country" === "Russia" && $"first_name".startsWith("M"))
    .explain()

  printHeader("Viewing a read with partition filters")
  val outputPathPartBy = outputLocation + "partitioning/partitionBy"
  spark.read.parquet(outputPathPartBy)
    .where($"country" === "Russia" && $"first_name".startsWith("M"))
    .explain()

}
