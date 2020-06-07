package c01_creating_dataframes

import org.apache.spark.sql.DataFrame
import utils.ExampleRunner

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.Row

object CreateDataFrames extends App with ExampleRunner  {
  import spark.implicits._


  printHeader("Creating a dataFrame using toDF")
  val df: DataFrame =
    Seq(
      ("Boston", "USA", 0.67),
      ("Dubai", "UAE", 3.1),
      ("Cordoba", "Argentina", 1.39)
    ).toDF("city","country","population")
  df.show()
  df.printSchema()


  printHeader("Adding a column to a DataFrame")
  df
    .withColumn("is_big_city", col("population") > 1)
    .show()


  printHeader("Filtering a DataFrame")
  df
    .filter(col("population") > 1)
    .show()


  printHeader("Creating a DataFrame with a custom schema")
  val animalData = Seq(Row(30, "bat"), Row(2, "mouse"), Row(25, "horse"))
  val animalSchema = List(
    StructField("average_lifespan", IntegerType, nullable = true),
    StructField("animal_type", StringType, nullable = true)
  )
  val animalDF = spark.createDataFrame(spark.sparkContext.parallelize(animalData), StructType(animalSchema))
  animalDF.show()
  animalDF.printSchema()

}
