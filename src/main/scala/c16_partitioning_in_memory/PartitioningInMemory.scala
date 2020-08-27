package c16_partitioning_in_memory

import utils.ExampleRunner

object PartitioningInMemory extends App with ExampleRunner  {

  import spark.implicits._

  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.functions.col

  val df = (1 to 10).toList.toDF("num")
  def printPartitionCount(df: DataFrame): Unit = {
    println(df.rdd.partitions.length)
  }
  def printPartitionContent(df: DataFrame): Unit = {
    println(df.rdd.glom.collect.map("Partition: " + _.mkString).mkString("\n"))
  }

  printHeader("Viewing the number of partitions")
  printPartitionCount(df)

  printHeader("\nViewing the structure of partitions")
  printPartitionContent(df)

  printHeader("\nReducing the number of partitions with coalesce:")
  printPartitionCount(df.coalesce(2))
  printPartitionContent(df.coalesce(2))

  printHeader("\nShuffling and increasing the number of partitions with repartition:")
  printPartitionCount(df.repartition(9))
  printPartitionContent(df.repartition(10))

  printHeader("\nRepartition by column (200 by default): ")
  val people = List(
    (10, "blue"),
    (13, "red"),
    (15, "blue"),
    (99, "red"),
    (67, "blue")
  ).toDF("age", "color")

  val peopleR = people.repartition(col("color"))
  printPartitionCount(peopleR)
  printPartitionContent(peopleR)
}
