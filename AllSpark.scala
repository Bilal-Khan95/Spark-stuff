import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

object AllSpark{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[*]").setAppName("main")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
//    This block of code was to test the Spark code, it was a simple table.

//    val rdd1 = sc.textFile("C:\\Users\\Admin\\Documents\\Bilals Stuff\\Bilal - Scala\\data.txt")
//    val topLine = rdd1.first()
//    val rdd2 = rdd1.filter(x => x!= topLine)
//    val rdd3 = rdd2.map(x => {
//      val rec = x.split(", ")
//      (rec(0).toInt, rec(1), rec(2), rec(3).toInt, rec(4))
//    })
//    val df = sqlContext.createDataFrame(rdd3).toDF("REGNO","NAME","SUBJECT","MARKS","CLIENT")
//    df.show()
//    df.select(df("NAME") === "Shafeeq").show

//    THIS BLOCK OF CODE USES MOVIE RATING DATA, THAT CONTAINS USER ID, MOVIE ID, THE RATING AND A TIMESTAMP.
    val rawMovieRdd = sc.textFile("C:\\Users\\Admin\\Downloads\\movies.txt")
    val structuredMovieRdd = rawMovieRdd.map(movie => {
      val rec = movie.split("\t")
      (rec(0).toInt, rec(1).toInt, rec(2).toInt, rec(3).toInt)
    })
    val structuredMovieRow = structuredMovieRdd.map(movie => Row(movie._1, movie._2, movie._3, movie._4))
    val schemaForStructuredMovieRow = StructType(
      StructField("user_id", IntegerType, true) ::
      StructField("movie_id", IntegerType, true) ::
      StructField("rating", IntegerType, true) ::
      StructField("timestamp", IntegerType, true) ::Nil
    )
    val movieDF = sqlContext.createDataFrame(structuredMovieRow, schemaForStructuredMovieRow)
    val fiveStarMovie = movieDF.filter(movieDF("rating") === 5).groupBy(movieDF("movie_id")).count()
    val bestFiveStarMovie = fiveStarMovie.orderBy(desc("count")).first()

//    THIS BLOCK GETS DATA FROM IMDB TO BE USED. THE ACTUAL USED DATA IS ONLY THE MOVIE ID AND ITS NAME.
    val movieNameAndId = sc.textFile("C:\\Users\\Admin\\Downloads\\movienames.txt")
    val strucutredMovieNameAndId = movieNameAndId.map(movie => {
      val rec = movie.split("\\|")
      (rec(0).toInt, rec(1))
    })
    val rowMovieNameAndId = strucutredMovieNameAndId.map(x => Row(x._1.toInt,x._2))
    val schemaForMovieNameAndId = StructType(
      StructField("movie_id", IntegerType, true) ::
      StructField("movie name & release year", StringType, true) ::Nil
    )
    val movieNameDF = sqlContext.createDataFrame(rowMovieNameAndId, schemaForMovieNameAndId)
//    movieNameDF.filter(movieNameDF("movie_id") === bestFiveStarMovie(0)).show()

//    THIS BLOCK OF CODE IS TESTING PURE SQL QUERIES THROUGH SPARK

    movieNameDF.registerTempTable("MovieNames")
//    sqlContext.sql("select * from Movienames where movie_id = 50").show()
    movieDF.registerTempTable("MovieData")
    sqlContext.sql("select movie_id from MovieData group by movie_id having count(*) = " +
      "(select max(C) from (select movie_id, count(*) as C from MovieData group by movie_id))").show()

//    THIS BLOCK OF CODE IS TO MAKE AND RUN USER DEFINED FUNCTION WITH PURE SQL AND WITH SCALA-SPARK

//    val dDouble =(s:Int) => s*2
//    val abc = udf(dDouble,IntegerType)
//    sqlContext.udf.register("abcSQLMode", dDouble)
//
//    sqlContext.range(1, 20).registerTempTable("test")
//    sqlContext.sql("select abcSQLMode(*) as Num from test").show()

  }
}