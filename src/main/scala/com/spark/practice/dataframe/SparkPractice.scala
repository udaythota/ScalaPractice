package com.spark.practice.dataframe

import org.apache.spark.sql.SparkSession

/**
  * Created by udaythota on 3/12/20.
  */
object SparkPractice {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[2]").appName("spark practice").getOrCreate()

    val input1 = spark.sparkContext.parallelize(1 to 10000, 25)
    println("The number of partitions in the rdd are: " + input1.getNumPartitions)

    // val input2 = spark.sparkContext.parallelize(1 to(100000, 17), 42).map(x => (x, x % 42)).collect()
    /*val input2 = spark.sparkContext.parallelize(1 to 10000, 42).map(x => (x, x % 42)).collect()
    println(input2.length)
    input2.foreach(a => println(a))*/

    val input2 = spark.sparkContext.parallelize(1 to 10000, 42).map(x => (x % 42, x))
    val def1 = input2.groupByKey().mapValues(_.sum)
    val def2 = input2.reduceByKey(_ + _)

    def2.collect().foreach(x => println(x))

    val inputFile = spark.sparkContext.textFile("src/main/resources/test_input.csv")

    // count of all individual words in the file
    val wordCount = inputFile
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey((x, y) => x + y)

    // total words in the file
    val wordCount1 = inputFile
      .flatMap(line => line.split(" "))
      .count()

    println(s"total number of words in the file is: $wordCount1")

    val greaterLineCount = inputFile.map(line => line.split(" ").length).reduce((x, y) => if (x > y) x else y)
    println("the count of words in the bigger line is: " + greaterLineCount)

    println(inputFile.count()) // this will only return 3 and not the number of words in the file, as this rdd contains 3 elements / 3 lines

    // instead of passing from the file, we are directly creating the rdd from parallelizing the array
    val text = Array("parallelising directly from the string variable instead of passing it from the file", "writing the same file again")
    val textRDD = spark.sparkContext.parallelize(text)

    val textRDDMap = textRDD.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    textRDDMap.foreach(println)

    // now to operate with integers
    val integerArray1 = Array(1, 2, 3, 4, 5, 6)
    val integerArray2 = Array(1, 3, 5)

    val rdd1 = spark.sparkContext.parallelize(integerArray1)
    val rdd2 = spark.sparkContext.parallelize(integerArray2)

    val commonElements = rdd1.intersection(rdd2)
    commonElements.foreach(println)
  }
}