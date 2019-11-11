package com.spark.practice.dataframe

import com.spark.practice.utils.Context;

/**
  * Created by udaythota on 2/5/19.
  */
object DataFrameOperations extends App with Context {

  val dfTags = sparkSession
    .read
    .option("header", "true")
    .csv("src/main/resources/question_tags_10K.csv")
    .toDF("id", "tag")

  dfTags.show(10)
  dfTags.printSchema()

  // select the specific columns from df
  dfTags.select("id", "tag").show(10)


  // filter the columns with this specific tag
  dfTags.filter("tag == 'php'").show(10)

  // display the count the rows of the data frame
  println(s"Number of tags with php = ${dfTags.filter("tag=='php'").count()}")

  // DataFrame Query: SQL like query
  dfTags.filter("tag LIKE 's%'").show(10)

  // DataFrame Query: Multiple filter chaining
  dfTags
    .filter("tag LIKE 's%'")
    .filter("id == 25 OR id == 108")
    .show(10)

  // DataFrame Query: SQL Group By
  dfTags
    .groupBy("tag")
    .count()
    .show(10)

  // DataFrame Query: SQL Group By with filter
  dfTags.groupBy("tag").count().filter("count > 5").show(10)


  // read the 2nd file
  val dfQuestionsCSV = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
    .csv("src/main/resources/questions_10K.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

  // spark wrongly inferred the column name schemas for few of the columns
  dfQuestionsCSV.printSchema()

  // explicitly specify the data types for the columns
  val dfQuestions = dfQuestionsCSV.select(
    dfQuestionsCSV.col("id").cast("integer"),
    dfQuestionsCSV.col("creation_date").cast("timestamp"),
    dfQuestionsCSV.col("closed_date").cast("timestamp"),
    dfQuestionsCSV.col("deletion_date").cast("date"),
    dfQuestionsCSV.col("score").cast("integer"),
    dfQuestionsCSV.col("owner_userid").cast("integer"),
    dfQuestionsCSV.col("answer_count").cast("integer")
  )

  dfQuestions.printSchema()
  dfQuestions.show(10)

  // to operate with only subset of questions
  val dfQuestionsSubSet = dfQuestions
    .filter("score > 400 and score < 410").toDF()

  // DataFrame Query: Join and select columns
  dfQuestionsSubSet
    .join(dfTags, "id")
    .select("owner_userid", "tag", "creation_date", "score")
    .show(10)

  // join explicit columns
  dfQuestionsSubSet
    .join(dfTags, dfTags("id") === dfQuestionsSubSet("id"))
    .show(10)

  // inner join
  dfQuestionsSubSet
    .join(dfTags, Seq("id"), "inner")
    .show()


  // left outer join
  dfQuestionsSubSet
    .join(dfTags, Seq("id"), "left_outer")
    .show(10)

  // get the disticnt items
  dfQuestionsSubSet
    .select("tag")
    .distinct()
    .show(10)
}
