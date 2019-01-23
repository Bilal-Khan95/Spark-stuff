import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object AllSpark{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[*]").setAppName("main")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val rdd1 = sc.textFile("C:\\Users\\Admin\\Documents\\Bilals Stuff\\Bilal - Scala\\data.txt")
    val topLine = rdd1.first()
    val rdd2 = rdd1.filter(x => x!= topLine)
    val rdd3 = rdd2.map(x => {
      val rec = x.split(", ")
      (rec(0).toInt, rec(1), rec(2), rec(3).toInt, rec(4))
    })
    val df = sqlContext.createDataFrame(rdd3).toDF("REGNO","NAME","SUBJECT","MARKS","CLIENT")
    df.show()
//    df.select(df("NAME") === "Shafeeq").show
  }
}