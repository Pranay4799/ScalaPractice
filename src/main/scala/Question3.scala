import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, initcap, sum, when}

object Question3 {
  def main(args:Array[String]):Unit= {

    val spark_conf = new SparkConf()
    spark_conf.set("spark.app.name","Question3")
    spark_conf.set("spark.master","local[*]")

    val spark = SparkSession.builder()
      .config(spark_conf)
      .getOrCreate()

  import spark.implicits._

    val workload = List(
      ("karthik", "ProjectA", 120),
      ("karthik", "ProjectB", 100),
      ("neha", "ProjectC", 80),
      ("neha", "ProjectD", 30),
      ("priya", "ProjectE", 110),
      ("mohan", "ProjectF", 40),
      ("ajay", "ProjectG", 70),
      ("vijay", "ProjectH", 150),
      ("veer", "ProjectI", 190),
      ("aatish", "ProjectJ", 60),
      ("animesh", "ProjectK", 95),
      ("nishad", "ProjectL", 210),
      ("varun", "ProjectM", 50),
      ("aadil", "ProjectN", 90)
    ).toDF("name", "project", "hours")

    val workloadbyemp = workload.groupBy("name").agg(sum(col("hours")).alias("total_hours"))

    val workloadstatus = workloadbyemp.select(initcap(col("name")).alias("name"),
      when(col("total_hours")>200,"Overloaded")
        .when(col("total_hours") > 100 && col("total_hours") <= 200, "Balanced")
        .otherwise("Underutilized").alias("loadstatus"))

    workloadstatus.groupBy("loadstatus").agg(count(col("name"))).show

  }


}
