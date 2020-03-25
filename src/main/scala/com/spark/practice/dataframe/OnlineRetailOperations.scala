package com.spark.practice.dataframe

import com.spark.practice.utils.Context
import java.sql.Timestamp

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Created by udaythota on 3/23/20.
  */
object OnlineRetailOperations extends App with Context {

  case class Stock(invoiceNumber: String, stockCode: String, description: String, quantity: Int, InvoiceDate: String, unitPrice: Double, customerId: Int, country: String)

  def getStockRdd(line: String): Stock = {
    val word = line.split(",")
    Stock(word(0), word(1), word(2), word(3).toInt, word(4), word(5).toDouble, word(6).toInt, word(7))
  }

  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sparkContext)

  val stocksDF = sparkSession.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/onlineRetail.csv")

  import sparkSession.implicits._
  import org.apache.spark.sql.functions._


  /*val stocks = sparkContext.textFile("src/main/resources/onlineRetail.csv")
  val stockSchema = stocks.first()

  val stocksRdd = stocks.filter(stock => stock != stockSchema).map(getStockRdd)
  println(stocksRdd.count())
  stocksRdd.take(5)*/

  stocksDF.printSchema()
  stocksDF.show(2)

  // 1. Count of stocks whose price is greater than 2.5 group by InvoiceNo
  stocksDF.filter(stocksDF("UnitPrice") > 2.5).groupBy(stocksDF("InvoiceNo")).count().show()

  // alternate way of doing it
  // stocksDF.filter($"UnitPrice" > 2.5).select($"InvoiceNo").groupBy("InvoiceNo").count().show()


  // 2. Details of stocks where customerID is null
  val stocksDFWithCustIdNull = stocksDF.select("*").filter(isnull(stocksDF("CustomerID")))

  stocksDFWithCustIdNull.show()

  // 3. Replace null customerID with INT MAX
  val stocksDFWithCustIdMax = stocksDF.withColumn("CustomerID", when(isnull(stocksDF("CustomerID")), String.valueOf(Integer.MAX_VALUE)).otherwise(stocksDF("CustomerID")))
  // alternate way of replacing the values
  val stocksDFWithCustIdMax2 = stocksDF.na.fill(Integer.MAX_VALUE, Seq("CustomerID"))
  stocksDFWithCustIdMax.show()
  stocksDFWithCustIdMax2.show()

  // 4. Details of stocks where description is null
  val stocksWithDescriptionNull = stocksDF.filter(isnull(stocksDF("Description")))
  stocksWithDescriptionNull.show()

  // 5. Replace null Description with 'No description present'
  val stocksWithDescriptionModified = stocksDF.withColumn("Description", when(isnull(stocksDF("CustomerID")), "No description present").otherwise(stocksDF("Description")))
  // alternate way of doing it
  val stocksWithDescriptionModified2 = stocksDF.na.fill("No description present", Seq("Description"))
  stocksWithDescriptionModified.show()
  stocksWithDescriptionModified2.show()

  // 6. Months with decreasing stocks in year 2011: just order / sort by months in the descending order of number of stocks
  val stocksWithUnixTimeStamp = stocksWithDescriptionModified.withColumn("timestamp", unix_timestamp(stocksWithDescriptionModified("InvoiceDate"), "MM/dd/yyyy HH:mm").cast("timestamp"))
  val stocksWithMonthAndYear = stocksWithUnixTimeStamp.withColumn("year", year(stocksWithUnixTimeStamp("timestamp"))).withColumn("month", month(stocksWithUnixTimeStamp("timestamp")))
  stocksWithMonthAndYear.show()

  val decreasingStocks = stocksWithMonthAndYear.filter(stocksWithMonthAndYear("year") === 2011).select("*").groupBy(stocksWithMonthAndYear("month")).count().sort("count")
  decreasingStocks.show()

  // 7. Use map function and get data frame containing only invoiceNo,StockCode,description,date,unit price and CustomerID of stocks in year 2011
  val customColumnsDF = stocksWithMonthAndYear.filter(stocksWithMonthAndYear("year") === 2011).rdd.map(row => {
    (row(0).toString, row(1).toString, row(2).toString, row.getAs[Timestamp]("timestamp"), row.getAs[Double]("UnitPrice"), row.getAs[Int]("CustomerID"))
  }).toDF("InvoiceNo", "StockCode", "Description", "InvoiceDate", "UnitPrice", "CustomerID")

  /*val customColumnsDF = stocksWithMonthAndYear.filter(stocksWithMonthAndYear("year") === 2011).rdd.map(row => {
    val invoiceNum = if (!row(0).equals(null)) row(0).toString else "null"
    val stockCode = if (!row(1).equals(null)) row(0).toString else "null"
    val description = if (!row(2).equals(null) || !row(2).equals("")) row(0).toString else "null"
    val timeStamp = row.getAs[Timestamp]("timestamp")
    val unitPrice = if (!row(5).equals(null)) row.getAs[Double]("UnitPrice") else 0.0
    val customerId = if (!row(6).equals(null)) row.getAs[Int]("CustomerID") else 0
    (invoiceNum, stockCode, description, timeStamp, unitPrice, customerId)
    // (row(0).toString, row(1).toString, row(2).toString, row.getAs[Timestamp]("timestamp"), row.getAs[Double]("UnitPrice"), row.getAs[Int]("CustomerID"))
  }).toDF("InvoiceNo", "StockCode", "Description", "InvoiceDate", "UnitPrice", "CustomerID")*/

  customColumnsDF.printSchema()
  customColumnsDF.show(10) // TODO: somehow this doesn't work when it is more than 10: debug later

  // 8. Stock which is most frequently produced each month in year 2011
  /*val Rdd = stocksWithMonthAndYear.filter($"year" === 2011).rdd.map(row => {
    (row(2).toString, row.getAs[Int]("month"))
  })
  val x = Rdd.map(x => ((x._1, x._2), 1)).reduceByKey(_ + _).groupBy(data => data._1._2)
    .map(data => (data._1, data._2.toList)).mapValues(getPopularProductInAMonth)

  x.map(data => (data._1.toInt, data._2)).sortByKey(true).foreach(println)*/


  // helper function
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


  // some random practice
  // approach 1: to flatmap on a data frame: data frame => rdd => flatmap => back to data frame
  val map = Map("a" -> List("c", "d", "e"), "b" -> List("f", "g", "h"))
  val df2 = List(("a", 1.0), ("b", 2.0)).toDF("x", "y")

  val rdd1 = df2.rdd.flatMap(row => {
    val x = row.getAs[String]("x")
    val y = row.getAs[Double]("y")
    for (value <- map(x)) yield Row(value, y)
  })
  rdd1.foreach(println)

  val newDF = sparkSession.createDataFrame(rdd1, df2.schema)
  newDF.show()


  // approach 2: join 2 data frames and explode the desired column
  val mapDF = Map("a" -> List("c", "d", "e"), "b" -> List("f", "g", "h")).toList.toDF("key", "list_values")
  val df3 = List(("a", 1.0), ("b", 2.0)).toDF("key", "value")

  val newDF2 = df3.join(mapDF, mapDF("key") === df3("key"), "inner")
    .select(mapDF("list_values"), df3("value"))
    .withColumn("list_values", explode(mapDF("list_values")))

  newDF2.show()
}
