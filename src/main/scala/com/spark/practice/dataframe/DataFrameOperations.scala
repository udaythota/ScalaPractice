package com.spark.practice.dataframe

import com.spark.practice.utils.Context
import org.apache.spark.sql.functions._

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
  /*dfQuestionsSubSet
    .select("tag")
    .distinct()
    .show(10)*/


  println("printing the df tags desired")
  // filter the columns with this specific tag
  dfTags.select("tag")
    .filter("tag like 'da%'")
    .show(10)

  dfTags.show(10)

  dfTags
    .where("id<10")
    .filter("tag LIKE 'cs%'")
    .agg(
      sum("id").as("sum_id"),
      avg("id").as("avg_id"))
    .show()

  // without this toDF function won't work
  import sparkSession.implicits._

  val simpleData = Seq(("James", "Sales", "NY", 90000, 34, 10000),
    ("Michael", "Sales", "NY", 86000, 56, 20000),
    ("Robert", "Sales", "CA", 81000, 30, 23000),
    ("Maria", "Finance", "CA", 90000, 24, 23000),
    ("Raman", "Finance", "CA", 99000, 40, 24000),
    ("Scott", "Finance", "NY", 83000, 36, 19000),
    ("Jen", "Finance", "NY", 79000, 53, 15000),
    ("Jeff", "Marketing", "CA", 80000, 25, 18000),
    ("Kumar", "Marketing", "NY", 91000, 50, 21000)
  )

  val df = simpleData.toDF("employee_name", "department", "state", "salary", "age", "bonus")
  df.show()

  df.groupBy("department")
    .sum("salary").as("salary_sum").show()

  df.groupBy("department")
    .mean("salary").as("salary_mean").show()

  df.filter("department=='Sales'")
    .groupBy("department")
    .sum("salary")
    .show()

  // to get multiple aggregates at the same time
  df.groupBy("department")
    .agg(
      sum("salary").as("sum_salary"),
      avg("age").as("avg_age"))
    .show()

  // to use data frame like hive table in spark sql
  df.createOrReplaceTempView("df_test")
  sparkSession.sql("select * from df_test limit 10").show()

  val sourceData = Seq(
    ("a", Seq("b", "c", "d", "e")),
    ("b", Seq("a", "c", "d")),
    ("c", Seq("a", "e"))
  )

  val mappingData = Seq(
    ("a", "true"),
    ("b", "false"),
    ("c", "true"),
    ("d", "false"),
    ("e", "true")
  )

  val sourceDF = sourceData.toDF("targetId", "sourceId")
  val mappingDF = mappingData.toDF("id", "available")

  /*  case class source(targetId: String, sourceId: Seq[String])

    case class mapping(id: String, available: String)

    case class sourceNew(targetId: String, source_id_split: String)

    val sourceDS = sourceDF.as[source]
    val mappingDS = mappingDF.as[mapping]

    val resultDS = sourceDS.select(col("targetId"), explode(col("sourceId")).as("source_id_split")).as[sourceNew]
    resultDS.show()

    resultDS.select("targetId").withColumn("targetAvailable", isAvailable(resultDS.col("targetId")))
      .withColumn("sourceAvailable", isAvailable(resultDS.col("source_id_split")))
      .show()*/


  val tempDF = sourceDF.select(col("targetId"), explode(col("sourceId")).as("source_id_split"))

  tempDF.show()
  val resultDF = tempDF.select("targetId", "sourceId").withColumn("targetAvailable", isAvailable(tempDF.col("targetId")))
    .withColumn("sourceAvailable", isAvailable(tempDF.col("source_id_split")))

  /*resultDF.select("targetId", "sourceId").
    filter(col("targetAvailable") === "true" and col("sourceAvailable") === "false").show()*/


  val isAvailable = udf((searchId: String) => {
    val rows = mappingDF.select("available").filter(col("id") === searchId).collect()
    if (rows(0)(0).toString.equals("true")) "true" else "false"
  })

  def isAvailable(searchId: String): String = {
    val rows = mappingDF.select("available").filter(col("id") === searchId).collect()
    if (rows(0).mkString.equals("true")) "true" else "false"
  }


  // trying with second approach: add mapping data to a map and map every value in data set based on that map
  /*val explodedSource = dataset1.select(col("targetId"), explode(col("sourceId")).as("source_id_split")).as[sourceNew] // same as dataset3
  explodedSource.show()
  dataset2.show()

  val list = List(List("test", "test1", "test2"), List("test3", "test4", "test5"))
  val flattenedMap = list.flatten.foreach(println)
  val flattenedMap2 = list.flatten.map(_.toUpperCase())
  // convert to capital letters
  val flattenedMap3 = list.flatten.map(_.capitalize) // converts only first letters in every word to capital letter

  val strings = Seq("1", "2", "foo", "3", "bar")

  def toInt(s: String): Option[Int] = {
    try {
      Some(Integer.parseInt(s.trim))
    } catch {
      case e: Exception => None
    }
  }

  // equivalent to strings.map(toInt)
  val strings1 = strings.map(word => toInt(word))
  println(strings.flatMap(toInt).sum)

  val chars = 'a' to 'z'
  val perms = chars.flatMap { a =>
    chars.flatMap { b =>
      if (a != b) Seq("%c%c".format(a, b))
      else Seq()
    }
  }
  println(perms)

  val acctDF = List(("1", "Acc1"), ("1", "Acc1"), ("1", "Acc1"), ("2", "Acc2"), ("2", "Acc2"), ("3", "Acc3")).toDF("AcctId", "Details")
  acctDF.show()
  acctDF.groupBy("AcctId").count().as("cnt").filter(col("count") > 1).select("AcctId").show()*/
}