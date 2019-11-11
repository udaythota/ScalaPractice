package com.spark.practice.dataframe

import com.spark.practice.utils.Context
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by udaythota on 11/5/19.
  */
object DFTest extends App with Context {

  case class ClickStream(user_id: String,
                         navigation_page: String,
                         url: String, session_id: Int,
                         date: String, hour: Int,
                         timestamp: Long)

  case class User(user_id: String, nav_list: Seq[String])

  // nav_pages is ordered as per the time sequence of events

  import sparkSession.implicits._

  val clickStreamDS = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("charset", "UTF8")
    .option("delimiter", ",")
    .csv("src/main/resources/netflix_input_data.csv")
    .as[ClickStream]

  clickStreamDS.show

  val window = Window.partitionBy("user_id").orderBy("timestamp")
  // when there are multiple executors does this still guarantee the result
  val sortedDF = clickStreamDS.withColumn("values_sorted_by_date", collect_list("navigation_page").over(window))
  sortedDF.show(false)

  val usersDS = sortedDF.groupBy("user_id")
    .agg(max("values_sorted_by_date").as("nav_list"))
    .as[User]

  usersDS.show(false)
  usersDS.printSchema()

  val titlePageList = Seq("OurPlanetTitle")

  usersDS.select("user_id")
    .where(size(col("nav_list")) === 1)
    .show(false)

  usersDS.withColumn("test", array_contains(col("nav_list"), "OriginalsGenre")).show(false)

  // 1) all users that visited 'OurPlanetTitle' page
  usersDS.filter(array_contains(col("nav_list"), "OurPlanetTitle"))
    .select("user_id")
    .orderBy("user_id")
    .show

  // 3) all users that visited 'OurPlanetTitle' page only once
  usersDS.select(col("user_id"), explode(col("nav_list")).as("title"))
    .filter("title == 'OurPlanetTitle'")
    .groupBy("user_id")
    .agg(count("title").as("title_count"))
    .filter(col("title_count") === 1)
    .select("user_id")
    .orderBy("user_id")
    .show

  // 5) all users who only visited 'OurPlanetTitle' page in the past
  usersDS.where(size(col("nav_list")) === 1)
    .filter(array_contains(col("nav_list"), "OurPlanetTitle"))
    .select("user_id")
    .show


}