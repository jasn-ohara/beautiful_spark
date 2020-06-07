name := "beautiful_spark"

version := "0.1"

scalaVersion := "2.12.7"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
                      "org.apache.spark" %% "spark-sql" % "2.4.5",
                      "mrpowers" % "spark-daria" % "0.35.0-s_2.12"
)
