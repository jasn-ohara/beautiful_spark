package c08_schemas

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.Row
import utils.ExampleRunner

object Schemas extends App with ExampleRunner  {

  import spark.implicits._
  val data = Seq(
    Row(8, "bat"),
    Row(64, "mouse"),
    Row(-27, "horse")
  )

  printHeader("Creating a DataFrame using a StructType schema")
  val schemaNormal = StructType(
    List(
      StructField("number", IntegerType, true),
      StructField("word", StringType, true)
    )
  )
  val dfNormal = spark.createDataFrame(spark.sparkContext.parallelize(data), schemaNormal)
  dfNormal.show()
  dfNormal.printSchema()

  printHeader("Creating a DataFrame using a StructType with a different delimiter")
  val schemaColon = StructType(
    StructField("number", IntegerType, true) ::
      StructField("word", StringType, true) :: Nil
  )
  val dfColon = spark.createDataFrame(spark.sparkContext.parallelize(data), schemaColon)
  dfColon.show()
  dfColon.printSchema()

  printHeader("Adding StructField to a StructType")
  val schemaAdd =
    StructType(Seq(StructField("number", IntegerType, true)))
      .add(StructField("word", StringType, true))
  val dfAdd = spark.createDataFrame(spark.sparkContext.parallelize(data), schemaAdd)
  dfAdd.show()
  dfAdd.printSchema()


  printHeader("A common mistake, an extra column in the schema")
  val schemaExtraCol = StructType(
    List(
      StructField("number", IntegerType, true),
      StructField("word", StringType, true),
      StructField("extra", StringType, true)
    )
  )
  try {
    spark.createDataFrame(spark.sparkContext.parallelize(data), schemaExtraCol).show()
  } catch {
    case e: Throwable => e.printStackTrace()
  }
}
