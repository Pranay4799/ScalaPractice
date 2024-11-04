import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, initcap, sum, when}

object Question2 {
  def main(args:Array[String]):Unit=
  {
    val spark_conf = new SparkConf()
    spark_conf.set("spark.app.name","Question2")
    spark_conf.set("spark.master","local[*]")

    val spark = SparkSession.builder()
      .config(spark_conf)
      .getOrCreate()

    import spark.implicits._

    val sales = List(
      ("karthik", 60000),
      ("neha", 48000),
      ("priya", 30000),
      ("mohan", 24000),
      ("ajay", 52000),
      ("vijay", 45000),
      ("veer", 70000),
      ("aatish", 23000),
      ("animesh", 15000),
      ("nishad", 8000),
      ("varun", 29000),
      ("aadil", 32000)
    ).toDF("name","total_sales")

    val performance_status = sales.select(initcap(col("name")),col("total_sales"),
      when(col("total_sales")>50000,"Excellent")
        .when(col("total_sales")>25000 && col("total_sales")<=50000,"Good")
        .otherwise("Needs Improvement").alias("status"))
    performance_status.show()

    performance_status.groupBy("status").agg(sum(col("total_sales"))).show()

  }

}
