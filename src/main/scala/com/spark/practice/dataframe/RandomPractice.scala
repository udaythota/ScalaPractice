package com.spark.practice.dataframe

import com.spark.practice.utils.Context
import org.apache.spark.SparkContext

/**
  * Created by udaythota on 3/24/20.
  */
object RandomPractice extends App with Context {
  val sparkContext = new SparkContext(sparkConf)
  val numbersFile = sparkContext.textFile("src/main/resources/sample_number_pairs.txt")
  // to covert from text file to rdd
  val pairRdd = numbersFile.map(line => (line.split(",")(0), line.split(",")(1))).groupByKey().sortByKey().map(pair => (pair._1, pair._2.mkString(",")))
  pairRdd.foreach(println)

  // alternate method of converting from text file to rdd
  val pairRdd2 = numbersFile.map(line => line.split(",")).map(array => (array(0), array(1))).groupByKey().sortByKey().map(pair => (pair._1, pair._2.mkString(",")))
  pairRdd2.foreach(println)

  import org.apache.spark.sql.functions._
  import sparkSession.implicits._

  case class Numbers(number1: Int, number2: Int)

  val df = numbersFile.map(line => line.split(",")).map {
    case Array(a, b) => Numbers(a.toInt, b.toInt)
  }.toDF("number1", "number2")
  df.show()

  // if you want the values of group by in an array
  df.groupBy(df("number1")).agg(collect_list(df("number2"))).show()

  // if you want them as comma separated values: first collect them to the list, and add a new column to replace the list column with concatenated string values (makes sure withColumn name matches the collect_list column)
  val df2 = df.groupBy(df("number1")).agg(collect_list(df("number2")).as("values"))
    .withColumn("values", concat_ws(",", col("values")))

  df2.show()

  // merge the data frame columns in to one column (mergedCols): basically adds all the columns to an array
  df2.withColumn("mergedCols", array(col("number1"), col("values"))).select("mergedCols").show()

  // merge the data frame columns in to one column (mergedCols): concat all the columns to string rather than to array
  val df3 = df2.withColumn("mergedCols", concat_ws(",", array(col("number1"), col("values")))).select("mergedCols")


  // flatmap this
  val lt = List(("some | random | value", 10),
    ("some | random | value", 11),
    ("some | random | value", 12))

  val convert: ((String, Int)) => List[(String, Int)] = tuple => tuple._1.split('|').map(str =>
    (str.trim, tuple._2)).toList

  lt.flatMap(convert).foreach(println)

  // question asked in apple 1st onsite
  val sourceDF = Seq("a" -> Seq("b", "c", "d", "e"), "b" -> Seq("a", "c", "d"), "c" -> Seq("a", "e")).toDF("targetId", "sourceId")
  val mappingDF = Seq("a" -> true, "b" -> false, "c" -> true, "d" -> false, "e" -> true).toDF("id", "available")

  /*val sourceDF = Seq("a" -> Seq("b", "c", "d", "e"), "b" -> Seq("a", "c", "d", "f"), "c" -> Seq("a", "e", "d", "f"), "f" -> Seq("b", "c", "d", "a")).toDF("targetId", "sourceId")
  val mappingDF = Seq("a" -> false, "b" -> true, "c" -> true, "d" -> true, "e" -> false, "f" -> true).toDF("id", "available")*/

  sourceDF.show()
  mappingDF.show()

  /*sourceDF
    // we can filter available targets before exploding.
    // let's do it to be more efficient.
    .join(mappingDF.withColumnRenamed("id", "targetId"), Seq("targetId"))
    .where('available)
    // exploding the sources
    .select('targetId, explode('sourceId) as "sourceId")
    // then we keep only non available sources
    .join(mappingDF.withColumnRenamed("id", "sourceId"), Seq("sourceId"))
    .where(!'available)
    .select("targetId", "sourceId")
    .show(false)*/

  // my initial brute force approach
  val targetValid = sourceDF.join(mappingDF, sourceDF("targetId") === mappingDF("id")).where(col("available") === true).select(col("targetId"), explode(col("sourceId")) as "exploded_col", col("available") as "targetAvailable")
  val sourceValid = targetValid.join(mappingDF, targetValid("exploded_col") === mappingDF("id")).where(mappingDF("available") === false).select(col("targetId"), col("exploded_col") as "sourceId")
  sourceValid.show()

  // optimized approach: first join with mapping table is filter out the targets with available = true and the second join again with mapping table is to filter out the sources with available = false
  val validMappings = sourceDF.join(mappingDF, sourceDF("targetId") === mappingDF("id")).where(mappingDF("available") === true).select(col("targetId"), explode(col("sourceId")) as "sourceId").join(mappingDF, col("sourceId") === mappingDF("id")).where(mappingDF("available") === false).select("targetId", "sourceId")
  validMappings.show()

  // find the intersection of elements in 2 lists
  val list1 = List(1, 2, 2, 2, 3, 4, 5, 5)
  val list2 = List(2, 3)

  val rdd1 = sparkContext.parallelize(list1)
  val rdd2 = sparkContext.parallelize(list2)

  val setRdd = rdd2.collect().toSet

  val mergedRdd = rdd1.filter(num => setRdd.contains(num)).distinct()
  mergedRdd.foreach(println)

  val fruits = List("apple", "apple", "orange", "apple", "mango", "orange")
  val word = fruits.flatMap(word => word.split(","))

  word.foreach(println)

  /* val firstDF = rdd1.toDF("num1")
   val secondDF = rdd2.toDF("num2")

   val commonDF = firstDF.join(secondDF, firstDF("num1") === secondDF("num2")).select(firstDF("num1")).distinct().sort()
   commonDF.show()*/

}