package c19_dependency_injection

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.broadcast

import utils.ExampleRunner


object DependencyInjection extends App with ExampleRunner {

  import spark.implicits._

  object Config{
    val test: Map[String, String] = Map(
      "stateMappingsPath" -> "src/test/data/dependencyInjection/stateMappings.csv"
    )
    val production: Map[String, String] = Map(
      "stateMappingsPath" -> "s3a://some-fake-bucket/state_mappings.csv"
    )

    // var environment = sys.env.getOrElse("PROJECT_ENV", "production")
    val environment = "test"
    def get(key: String): String = {
      if (environment == "test"){
        test(key)
      } else {
        production(key)
      }
    }
  }

  printHeader("Injecting a path")
  def withStateFullNameInjectPath(stateMappingsPath: String = Config.get("stateMappingsPath")
                                 )(df: DataFrame): DataFrame = {
    val stateMappingsDf = spark.read.option("header", "true").csv(stateMappingsPath)
    df
      .join(broadcast(stateMappingsDf),
        df("state") <=> stateMappingsDf("state_abbreviation"),
        joinType= "left_outer")
      .drop("state_abbreviation")
  }
  val df = Seq(
    ("John", 23, "TN"),
    ("Sally", 48, "NY")
  ).toDF("name", "age", "state")

  df.transform(withStateFullNameInjectPath())
    .show()

  printHeader("Injecting a DataFrame")
  def withStateFullNameInjectDf(stateMappingsDf: DataFrame =
                                spark
                                    .read
                                    .option("header", "true")
                                    .csv(Config.get("stateMappingsPath"))
                                 )(df: DataFrame): DataFrame = {
    df
      .join(broadcast(stateMappingsDf),
        df("state") <=> stateMappingsDf("state_abbreviation"),
        joinType= "left_outer")
      .drop("state_abbreviation")
  }
  df.transform(withStateFullNameInjectDf())
    .show()

}
