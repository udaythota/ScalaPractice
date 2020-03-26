package com.spark.practice.dataframe

import com.spark.practice.dataframe.RandomPractice.{sparkConf, sparkSession}
import org.apache.spark.SparkContext

/**
  * Created by udaythota on 3/25/20.
  */
object MovieLensAnalytics extends App {
  val sparkContext = new SparkContext(sparkConf)
  val moviesFile = sparkContext.textFile("src/main/resources/movies.dat")
  val usersFile = sparkContext.textFile("src/main/resources/users.dat")
  val ratingsFile = sparkContext.textFile("src/main/resources/ratings.dat")

  moviesFile.take(3).foreach(println)
  ratingsFile.take(3).foreach(println)

  // top viewed movies: sort by count descending order
  val topViewedMovies = ratingsFile.map(rating => rating.split("::")).map(array => (array(1), 1)).reduceByKey(_ + _).sortBy(_._2, false)
  topViewedMovies.top(10).foreach(println)

  // list of distinct genres
  val distinctGenres = moviesFile.map(movie => movie.split("::")(2)).flatMap(line => line.split('|')).distinct()
  distinctGenres.foreach(println)

  // movies per each genre. TODO: how to find all the movies under each genre. this will be an interesting question
  val genres = moviesFile.map(movie => movie.split("::")(2))
  val flatGenres = genres.flatMap(genre => genre.split('|'))
  val moviesPerGenre = flatGenres.map(genre => (genre, 1)).reduceByKey(_ + _).sortBy(_._1)
  moviesPerGenre.foreach(println)

  // movies starting with numbers or letters: TODO
  val moviesStarting = moviesFile.map(movie => movie.split("::")(1)).filter(movie => movie.startsWith("^[A-Z0-9]$"))
  println(moviesStarting.count())
  moviesStarting.take(20).foreach(println)

  // list of latest released movies
  val movies = moviesFile.map(line => line.split("::")(1))
  val latestYear = movies.map(movie => movie.substring(movie.lastIndexOf("(") + 1, movie.lastIndexOf(")"))).max()
  println("the latest year of the all the movies released is: " + latestYear)
  val latestReleasedMovies = movies.filter(movie => movie.contains("(" + latestYear + ")")) // as year could be part of the movie name itself, we need to make sure we filter the year present only in the year place holder eg:(2000)
  println(latestReleasedMovies.count())
  latestReleasedMovies.take(10).foreach(println)

  import sparkSession.implicits._

  // part 2: spark SQL
  val moviesDF = moviesFile.toDF()
  moviesDF.createOrReplaceTempView("moviesView")

  // list of oldest released movies: movies which got released in the oldest available year
  val moviesFiltered = sparkSession.sql("select split(value, '::')[0] as movieId, split(value, '::')[1] as movieName, substring(split(value, '::')[1], length(split(value, '::')[1])-4, 4) as year from moviesView").createOrReplaceTempView("moviesView")
  val oldestMovies = sparkSession.sql("select movieName from moviesView m1 where m1.year = (select min(m2.year) from moviesView m2)")
  oldestMovies.show()

  // create tables from the files
  val tempMoviesView = sparkSession.read.textFile("src/main/resources/movies.dat").createOrReplaceTempView("movies_staging")
  val tempRatingsView = sparkSession.read.textFile("src/main/resources/ratings.dat").createOrReplaceTempView("ratings_staging")
  val tempUsersView = sparkSession.read.textFile("src/main/resources/users.dat").createOrReplaceTempView("users_staging")

  // create the movies table
  sparkSession.sql("create database if not exists movies_db")
  val moviesTable = sparkSession.sql("select split(value, '::')[0] as movieId, split(value, '::')[1] as movieName, substring(split(value, '::')[1], length(split(value, '::')[1])-4, 4) as year from movies_staging").write.mode("overwrite").saveAsTable("movies_db.movies")
  sparkSession.sql("select * from movies_db.movies limit 5").show()

  // create the ratings table
  sparkSession.sql("create database if not exists ratings_db")
  val ratingsTable = sparkSession.sql("select split(value, '::')[0] as userId, split(value, '::')[1] as movieId, split(value, '::')[2] as rating, split(value, '::')[3] as timestamp from ratings_staging").write.mode("overwrite").saveAsTable("movies_db.ratings")
  sparkSession.sql("select * from movies_db.ratings limit 5").show()

  // similarly create the users table
  sparkSession.sql("create database if not exists ratings_db")
  val userTable = sparkSession.sql("select split(value, '::')[0] as userId, split(value, '::')[1] as gender, split(value, '::')[2] as age, split(value, '::')[3] as occupation, split(value, '::')[4] as zip from users_staging").write.mode("overwrite").saveAsTable("movies_db.user")
  sparkSession.sql("select * from movies_db.user limit 5").show()

  // number of movies released every year
  val moviesReleasedEachYear = sparkSession.sql("select year, count(*) as movie_count from movies_db.movies group by year order by year")
  moviesReleasedEachYear.show()

  // NOTE: if you want to write / save it to the file
  moviesReleasedEachYear.repartition(1).write.format("csv").option("header", "true").save("result")

  // number of movies per each rating
  val moviesPerRating = sparkSession.sql("select rating, count(*) as rating_count from movies_db.ratings group by rating order by rating")
  moviesPerRating.show()


  // number of users rated for each movie
  val usersRated = sparkSession.sql("select movieId, count(distinct(userId)) as users_rated from movies_db.ratings group by movieId")
  usersRated.show()

  // total rating for each movie
  val totalRating = sparkSession.sql("select movieId, sum(rating) as total_rating_sum from movies_db.ratings group by movieId order by movieId")
  totalRating.show()

  // average rating for each  movie
  val averageRating = sparkSession.sql("select movieId, cast((avg(rating)) as decimal(16, 2)) as avg_rating_sum from movies_db.ratings group by movieId order by movieId")
  averageRating.show()


  // part 3: data frames: clean data in to data frame
  // TODO: https://github.com/Thomas-George-T/MoviesLens-Analytics-in-Spark-and-Scala/blob/master/Spark_DataFrames/Clean.scala

}
