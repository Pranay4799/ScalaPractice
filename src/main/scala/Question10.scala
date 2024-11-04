import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, initcap, sum, when}

object Question10 {
  def main(args:Array[String]): Unit = {
    val spark_conf = new SparkConf()
    spark_conf.set("spark.app.name", "Question8")
    spark_conf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(spark_conf)
      .getOrCreate()
    import spark.implicits._

    val employees = List(
      ("karthik", "Sales", 85),
      ("neha", "Marketing", 78),
      ("priya", "IT", 90),
      ("mohan", "Finance", 65),
      ("ajay", "Sales", 55),
      ("vijay", "Marketing", 82),
      ("veer", "HR", 72),
      ("aatish", "Sales", 88),
      ("animesh", "Finance", 95),
      ("nishad", "IT", 60)
    ).toDF("name", "department", "performance_score")


    val category = employees.select(col("name"),col("department"),
      when(col("performance_score")>80 && (col("department") ==="Sales" || col("department") ==="Marketing"),0.20)
        .when(col("performance_score")>70,0.15)
        .otherwise(0).alias("bonus"))
    category.groupBy("department").agg(sum(col("bonus"))).show()

  }
}
