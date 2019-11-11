package com.spark.practice.dataframe

import com.spark.practice.utils.Context
import org.apache.spark.sql.functions._


/**
  * Created by udaythota on 2/7/19.
  */
object SparkSqlOperations extends App with Context {

  val dfTags = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/question_tags_10K.csv")
    .toDF("id", "tag")

  // Create a dataframe from questions file questions_10K.csv
  val dfQuestionsCSV = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
    .csv("src/main/resources/questions_10K.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

  // cast columns to data types
  val dfQuestions = dfQuestionsCSV.select(
    dfQuestionsCSV.col("id").cast("integer"),
    dfQuestionsCSV.col("creation_date").cast("timestamp"),
    dfQuestionsCSV.col("closed_date").cast("timestamp"),
    dfQuestionsCSV.col("deletion_date").cast("date"),
    dfQuestionsCSV.col("score").cast("integer"),
    dfQuestionsCSV.col("owner_userid").cast("integer"),
    dfQuestionsCSV.col("answer_count").cast("integer")
  )

  // register as a temp table
  dfTags.createOrReplaceTempView("so_tags")

  // list all the tables in the spark catalog
  sparkSession.catalog.listTables().show()

  sparkSession.sql("show tables").show()

  // test
  sparkSession.sql("select id, tag from so_tags").show(3)

  // count
  sparkSession
    .sql("select count(*) as php_tags from so_tags where tag='php'".stripMargin)
    .show(10)


  // register udf and use it in the query
  def prefixStackOverflow(s: String): String = s"so_$s"

  sparkSession
    .udf
    .register("prefix_so", prefixStackOverflow _)

  sparkSession
    .sql("select id, prefix_so(tag) from so_tags".stripMargin)
    .show(10)


  dfQuestions
    .filter("id > 400 and id < 450")
    .filter("owner_userid is not null")
    .join(dfTags, dfQuestions.col("id").equalTo(dfTags("id")))
    .groupBy(dfQuestions.col("owner_userid"))
    .agg(avg("score"), max("answer_count"))
    .show(10)


  dfQuestions.show(10)

  // DataFrame Statistics using describe() method
  // TODO: where are the other columns from dfQuestions??
  val dfQuestionsStatistics = dfQuestions.describe()
  dfQuestionsStatistics.show()

}
