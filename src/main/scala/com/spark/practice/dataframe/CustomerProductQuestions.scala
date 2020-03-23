package com.spark.practice.dataframe

import com.spark.practice.utils.Context
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.Window

/**
  * Created by udaythota on 3/22/20.
  */
object CustomerProductQuestions extends App with Context {

  case class Customer(customerId: Int, name: String)

  case class Product(itemId: Int, category: String, productNumber: Int, name: String)

  case class Sale(transactionId: Int, customerId: Int, itemId: Int, amount: Option[Double], date: String)

  /*val customers = sparkSession.read
    .option("header", "true")
    .csv("src/main/resources/customers.csv")
    .toDF("customerId", "name")

  val products = sparkSession.read
    .option("header", "true")
    .csv("src/main/resources/products.csv")
    .toDF("itemId", "category", "productNumber", "name")

  val sales = sparkSession.read
    .option("header", "true")
    .csv("src/main/resources/sales.csv")
    .toDF("transactionId", "customerId", "itemId", "amount", "date")


  println(customers.count())
  println(products.count())
  println(sales.count())

  customers.show()
  sales.show()*/

  def getCustomerRdd(line: String): Customer = {
    val word = line.split(",")
    Customer(word(0).toInt, word(1))
  }

  def getProductRdd(line: String): Product = {
    val word = line.split(",")
    Product(word(0).toInt, word(1), word(2).toInt, word(3))
  }

  def getSalesRdd(line: String): Sale = {
    val word = line.split(",")
    var amount: Option[Double] = Some(0)
    if (word(3) == null || word(3) == "") amount = null else amount = Some(word(3).toDouble)
    Sale(word(0).toInt, word(1).toInt, word(2).toInt, amount, word(4))
  }

  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sparkContext)

  import sparkSession.implicits._
  import org.apache.spark.sql.functions._

  val products = sparkContext.textFile("src/main/resources/products.csv")
  val customers = sparkContext.textFile("src/main/resources/customers.csv")
  val sales = sparkContext.textFile("src/main/resources/sales.csv")

  val productSchema = products.first()
  val productsRdd = products.filter(line => line != productSchema).map(getProductRdd)

  val customerSchema = customers.first()
  val customerRdd = customers.filter(line => line != customerSchema).map(getCustomerRdd)

  val salesSchema = sales.first()
  val salesRdd = sales.filter(line => line != salesSchema).map(getSalesRdd)

  val products_Df = productsRdd.toDF()
  val customers_Df = customerRdd.toDF()
  val sales_Df = salesRdd.toDF()

  println("count of products: " + products_Df.count())
  println("count of sales: " + sales_Df.count())
  println("count of customers: " + customers_Df.count())

  // val meanVal = sales_Df.select(sales_Df("amount")).agg(mean("amount"))
  val meanVal = sales_Df.agg(mean("amount")).collect()
  val meanValDouble = meanVal(0)(0).toString.toDouble
  // equivalent to meanVal(0)
  val round = Math.round(meanValDouble * 100.0) / 100.0

  // task: 3
  val modifiedSalesDF = sales_Df.select("*").withColumn("meanCol", when(isnull(col("amount")), round).otherwise(col("amount")))
  modifiedSalesDF.show()

  // customer sales join
  val customer_sales = customers_Df.join(modifiedSalesDF, Seq("customerId", "customerId")).select(customers_Df("customerId"), col("itemId"), col("date"), col("name"))

  val customer_sales_join = customers_Df.join(modifiedSalesDF, "customerId").
    withColumn("customerName", customers_Df("name")).drop(customers_Df("name"))

  // task 4: add sequential id's to data frames
  val customer_sales_seq_join = customer_sales.withColumn("sequential_row", monotonically_increasing_id()).show()

  // an other way is to do it using the window functions. this guarantees the first number is 1 and the numbers are sequential rather than monotonically_increasing_id which doesn't guarentee that
  val order = Window.orderBy("itemId")
  val joined_Table = customer_sales_join.withColumn("id", row_number().over(order))
  joined_Table.show()

  // can also use the zip with index - looks like more optimal

  // task 5: sales by week
  val cust_sales_with_date = joined_Table.withColumn("date", joined_Table("date").cast("date"))
  cust_sales_with_date.show()

  val cust_sales_with_week = cust_sales_with_date.withColumn("week", weekofyear(cust_sales_with_date("date")))

  val sales_per_week = cust_sales_with_week.groupBy("week").agg(sum("amount").as("sum_sales_week"))
  sales_per_week.show()

  // tasl 6: 5% discount amount
  val sales_with_discount = cust_sales_with_week.withColumn("discounted_amount", cust_sales_with_week("amount") * 0.05)

  sales_with_discount.show()
}