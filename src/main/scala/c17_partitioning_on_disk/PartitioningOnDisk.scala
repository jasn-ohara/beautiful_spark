package c17_partitioning_on_disk

import utils.ExampleRunner
import org.apache.spark.sql.functions.{col, rand}

object PartitioningOnDisk extends App with ExampleRunner  {

  import spark.implicits._
  val df = Seq(
    ("Ernesto", "Guevara", "Argentina"),
    ("Vladimir", "Putin", "Russia"),
    ("Maria", "Sharapova", "Russia"),
    ("Bruce", "Lee", "China"),
    ("Jack", "Ma", "China")
  ).toDF("first_name", "last_name", "country")
  df.show()

  printHeader("Partitioning by column")
  val outputPathPartBy = outputLocation + "partitioning/partitionBy"
  df
    .repartition(col("country"))
    .write
    .mode("Overwrite")
    .partitionBy("country")
    .parquet(outputPathPartBy)
  printDirContents(outputPathPartBy)

  printHeader("Repartition and partitionBy 5")
  val outputPathRepart5 = outputLocation + "partitioning/partitionByWithRepartition5"
  df
    .repartition(5)
    .write
    .mode("Overwrite")
    .partitionBy("country")
    .parquet(outputPathRepart5)
  printDirContents(outputPathRepart5 + "/country=China/")


  printHeader("Repartition and partitionBy 1")
  val outputPathRepart1 = outputLocation + "partitioning/partitionByWithRepartition1"
  df
    .repartition(1)
    .write
    .mode("Overwrite")
    .partitionBy("country")
    .parquet(outputPathRepart1)
  printDirContents(outputPathRepart1 + "/country=China")

  printHeader("Writing with a single partition")
  val outputPathSinglePartition =  outputLocation + "partitioning/singlePartition"
  df
    .repartition(1)
    .write
    .mode("Overwrite")
    .parquet(outputPathSinglePartition)
  printDirContents(outputPathSinglePartition)


  printHeader("Writing partitioned data with file limit per partition")
  val outputLimitedFiles =  outputLocation + "partitioning/limitedFiles"
  spark
    .read
    .option("header", "true")
    .csv(outputLocation + "partitioning/person_data.csv")
    .repartition(50, $"person_country", rand)
    .write
    .option("header", "true")
    .partitionBy("person_country")
    .mode("Overwrite")
    .parquet(outputLimitedFiles)
  printDirContents(outputLimitedFiles + "/person_country=China/")

}
