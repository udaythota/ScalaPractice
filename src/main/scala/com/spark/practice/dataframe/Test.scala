package com.spark.practice.dataframe

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by udaythota on 3/23/20.
  */
object Test {
  def getPopularProductInAMonth(list: List[((String, Int), Int)]): (String) = {
    var popularProductCount = 0
    var popularProductDescription = ""
    for (l <- list) {
      val count = l._2
      if (count > popularProductCount) {
        popularProductCount = count
        popularProductDescription = l._1._1
      }
    }
    (popularProductDescription)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val config = new SparkConf().setAppName("OnlineRetail").setMaster("local[*]")
      .set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts", "true");

    val sc = new SparkContext(config)

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val df = spark.read.option("header", true).option("inferSchema", true).csv("src/main/resources/onlineRetail.csv")

    df.printSchema()

    // 1. Count of stocks whose UnitPrice is greater than 2.5 group by InvoiceNo
    df.filter($"UnitPrice" > 2.5).select($"InvoiceNo").groupBy("InvoiceNo").count().show()

    // 2. Details of stocks where customerID is null
    df.filter(isnull($"CustomerID")).select("*").show(5)

    // 3. Replace null customerID with INT MAX
    val updated_customerId_df = df.na.fill(Int.MaxValue, Array("CustomerID"))
    updated_customerId_df.show(5)

    // 4. Details of stocks where description is null
    updated_customerId_df.filter(isnull($"Description")).select("*").show(5)

    // 5. Replace null Description with 'No description present'
    val updated_description_df = updated_customerId_df.na.fill("No description present", Array("Description"))
    updated_description_df.filter($"InvoiceNo" === "536414").show()

    // 6. Months with decreasing stocks in year 2011
    val table = updated_description_df.withColumn("InvoiceDate", unix_timestamp(updated_description_df("InvoiceDate"), "MM/dd/yyyy HH:mm").cast("timestamp"))
    table.printSchema()
    val updated_table = table.withColumn("month", month(table("InvoiceDate"))).withColumn("year", year(table("InvoiceDate")))
    updated_table.show(5)
    updated_table.filter($"year" === 2011).select("*").groupBy($"month").count().sort(desc("count")).show()

    // 7. Use map function and get data frame containing only invoiceNo,StockCode,description,date,unit price and CustomerID of stocks in year 2011
    val mapped_table = updated_table.filter($"year" === 2011).rdd.map(row => {
      (row(0).toString(), row(1).toString, row(2).toString(), row.getAs[Timestamp]("InvoiceDate"), row.getAs[Double]("UnitPrice"), row.getAs[Int]("CustomerID"))
    }).toDF("InvoiceNo", "StockCode", "Description", "InvoiceDate", "UnitPrice", "CustomerID")

    mapped_table.printSchema()
    mapped_table.show()

    // 8. Stock which is most frequently produced each month in year 2011
    val Rdd = updated_table.filter($"year" === 2011).rdd.map(row => {
      (row.getAs[String]("Description"), row.getAs[Int]("month"))
    })

   /* Rdd.toDF("test1", "test2").show()

    val descMonthDF = Rdd.map(x => ((x._1, x._2), 1))
    val descEachMonthDF = descMonthDF.reduceByKey(_ + _)

    val tempDF = descEachMonthDF.groupBy(data => data._1._2).map(data => (data._1, data._2.toList))
    tempDF.toDF().show()
    tempDF.take(2).foreach(println)*/

    val x = Rdd.map(x => ((x._1,x._2),1)).reduceByKey(_ + _).groupBy(data => data._1._2)
      .map(data => (data._1,data._2.toList)).mapValues(getPopularProductInAMonth)

    x.toDF().show()

    x.map(data => (data._1.toInt,data._2)).sortByKey(true).foreach(println)
  }
}
