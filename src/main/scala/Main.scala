import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("cricket").getOrCreate()
    val data = spark.read.options(Map("inferSchema"->"true", "header"->"true")).csv("src/main/resources/cricket.csv")
    val res = data.withColumn("over", ceil(col("ball")/6)).groupBy("over").agg(sum("run").as("runs"))
    res.show(100)
  }
}
