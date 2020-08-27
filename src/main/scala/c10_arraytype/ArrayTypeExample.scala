package c10_arraytype

import utils.ExampleRunner
import org.apache.spark.sql.functions._

object ArrayTypeExample extends App with ExampleRunner  {
  import spark.implicits._

  printHeader("Splitting a string into an ArrayType column")
  Seq(
    ("beatles", "help|hey jude"),
    ("romeo", "eres mia")
  )
    .toDF("name", "hit_songs")
    .withColumn("hit_songs", split($"hit_songs", "\\|"))
    .show()

  printHeader("Creating an ArrayType directly")
  Seq(
    ("beiber", Array("baby", "sorry")),
    ("ozuna", Array("criminal"))
  )
    .toDF("name", "hit_songs")
    .show()

  printHeader("Checking values in an array")
  val colorDf =
    Seq(
      ("bob", Array("red", "blue")),
      ("maria", Array("green")),
      ("sue", Array("red"))
    ).toDF("name", "favourite_colors")

  colorDf
    .withColumn("likes_red", array_contains($"favourite_colors", "red"))
    .show()

  printHeader("Exploding an array")
  colorDf
    .select(
      $"name",
      explode($"favourite_colors") as "color"
    ).show()

  printHeader("Collecting an array")
  Seq(
    ("a","b",1),
    ("a","b",2),
    ("a","b",3),
    ("z","b",4),
    ("a","x",5)
  )
    .toDF("l1", "l2", "n")
    .groupBy("l1", "l2")
    .agg(collect_list("n") as "collected")
    .show()

  printHeader("Single column array functions")
  Seq(Array(1, 2), Array(1, 2, 3, 1), null)
    .toDF("nums")
    .withColumn("distinct", array_distinct($"nums"))
    .withColumn("joined", array_join($"nums", "|"))
    .withColumn("max", array_max($"nums"))
    .withColumn("min", array_min($"nums"))
    .withColumn("1_removed", array_remove($"nums", 1))
    .withColumn("sorted", array_sort($"nums"))
    .show()

  printHeader("Multiple column array functions")
  Seq(
    (Array(1, 2), Array(4, 5, 6)),
    (Array(1, 2, 3, 1), Array(2, 3, 4)),
    (null, Array(6, 7))
  )
    .toDF("nums1", "nums2")
    .withColumn("nums_intersection", array_intersect($"nums1", $"nums2"))
    .withColumn("nums_union", array_union($"nums1", $"nums2"))
    .withColumn("1_except_2", array_except($"nums1", $"nums2"))
    .show()

  printHeader("Splitting arrays")
  Seq(Array("a", "b", "c"), Array("d", "e", "f"), null)
    .toDF("letters")
    .select(
      (0 until 3)
        .map(i => $"letters".getItem(i) as s"col$i"): _*
    )
    .show
}
