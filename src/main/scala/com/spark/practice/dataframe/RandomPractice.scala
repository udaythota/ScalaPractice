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

  import sparkSession.implicits._
  import org.apache.spark.sql.functions._

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

}