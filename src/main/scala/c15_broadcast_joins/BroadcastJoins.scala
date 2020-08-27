package c15_broadcast_joins

import utils.ExampleRunner

object BroadcastJoins extends App with ExampleRunner  {
  import spark.implicits._

  import org.apache.spark.sql.functions.broadcast

  printHeader("Example of a broadcast join:")

  val peopleDf = Seq(
    ("andrea", "medellin"),
    ("rodolfo", "medellin"),
    ("abdul", "bangalore")
  ).toDF("first_name", "city")

  val citiesDf = Seq(
    ("medellin", "colombia", 2.5),
    ("bangalore", "india", 12.3)
  ).toDF("city", "country", "population")

  val bcastJoin =
    peopleDf
      .join(
        citiesDf,
        peopleDf("city") <=> citiesDf("city")
      )
  bcastJoin.show()
  bcastJoin.explain()


  printHeader("\nExample of a manual broadcast join:")
  val manualBcastJoin =
    peopleDf
      .join(
        broadcast(citiesDf),
        peopleDf("city") <=> citiesDf("city")
      )
  manualBcastJoin.show()
  manualBcastJoin.explain()


  printHeader("\nExample of removing duplicate join columns with inefficient shortcut:")
  val noDupesJoinBad =
    peopleDf
      .join(
        broadcast(citiesDf),
        Seq("city")
      )
  noDupesJoinBad.show()
  noDupesJoinBad.explain()

  printHeader("\nExample of removing duplicate join columns more efficiently:")
  val noDupesJoinGood =
    peopleDf
      .join(
        broadcast(citiesDf),
        peopleDf("city") <=> citiesDf("city")
      ).drop(citiesDf("city"))
  noDupesJoinGood.show()
  noDupesJoinGood.explain()


  printHeader("\nExample of a full logical and physical execution plan:")
  noDupesJoinGood.explain(true)
}
