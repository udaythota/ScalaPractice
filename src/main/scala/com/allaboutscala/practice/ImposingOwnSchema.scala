package com.allaboutscala.practice

import org.apache.spark.sql.SparkSession

/**
  * Created by udaythota on 3/21/20.
  */
object ImposingOwnSchema extends App {

  val spark = SparkSession.builder()
    .master("local")
    .appName("Imposing our own schema")
    .getOrCreate()

  val namesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Users/udaythota/Downloads/Baby_Names__Beginning_2007.csv")


  namesDF.printSchema()
}
