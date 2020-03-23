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

  case class User(user_id: String, nav_list: List[String])

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

  // 2) all users that visited 'OurPlanetTitle' page only once
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

  usersDS.show(false)

  def filter_nav_list(nav_list: List[String]): Boolean = {
    for (i <- 0 until nav_list.length - 3) {
      if (nav_list(i).equals("HomePage") &&
        nav_list(1 + i).equals("OriginalsGenre") &&
        nav_list(2 + i).equals("OurPlanetTitle") &&
        nav_list(3 + i).equals("HomePage")
      )
        return true
    }
    return false

  }

  usersDS.rdd.map(x => x)
    .filter(x => filter_nav_list(x.nav_list)).toDF().show(false)

  usersDS.show(false)

  /*val w = window.partitionBy("user_id").orderBy("timestamp")
  val raw_users = clickStreamDS.select($"user_id", $"navigation_page".as("nav_page"), rank.over(w).as("ranker"))
  raw_users.show(false)

  //  val d1 = raw_users.as("d1")
  raw_users.as("d1").join(raw_users.as("d2"),
    $"d1.ranker" === $"d2.ranker" - 1
      and $"d1.user_id" === $"d2.user_id"
  ).join(raw_users.as("d3"),
    $"d2.ranker" === $"d3.ranker" - 1
      and $"d2.user_id" === $"d3.user_id"
  ).join(raw_users.as("d4"),
    $"d3.ranker" === $"d4.ranker" - 1
      and $"d3.user_id" === $"d4.user_id"
  )
    .filter($"d1.nav_page" === "HomePage"
      and $"d2.nav_page" === "OriginalsGenre" and $"d3.nav_page" === "OurPlanetTitle" and $"d4.nav_page" === "HomePage"
       )
      .show(100, false)*/
  //    .select($"d1.user_id", $"d1.nav_page", $"d2.nav_page", $"d3.nav_page", $"d4.nav_page")
  //    .select($"d1.user_id").distinct.show()

}


/**
  * +-------+--------------+------+
  * |user_id|nav_page      |ranker|
  * +-------+--------------+------+
  * |4001   |OurPlanetTitle|1     |
  * |3003   |OurPlanetTitle|1     |
  * |3003   |LogIn         |2     |
  * |1001   |OurPlanetTitle|1     |
  * |1001   |OriginalsGenre|2     |
  * |1001   |HomePage      |3     |
  * |1001   |HomePage      |4     |
  * |2002   |OriginalsGenre|1     |
  * |2002   |HomePage      |2     |
  * |2002   |HomePage      |3     |
  * |2002   |OriginalsGenre|4     |
  * |2002   |OurPlanetTitle|5     |
  * |2002   |HomePage      |6     |
  * |2002   |OurPlanetTitle|7     |
  * +-------+--------------+------+
  */