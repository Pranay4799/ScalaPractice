import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, current_date, current_timestamp, datediff, initcap, max, min, sum, to_date, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Problem1 {
  def main(args:Array[String]):Unit=
    {
  val spark_conf = new SparkConf()
      spark_conf.set("spark.app.name","Problem")
      spark_conf.set("spark.master","local[*]")
      val spark = SparkSession.builder()
        .config(spark_conf)
        .getOrCreate()

      import spark.implicits._

      val employees = List(
        ("karthik", "2024-11-01"),
        ("neha", "2024-10-20"),
        ("priya", "2024-10-28"),
        ("mohan", "2024-11-02"),
        ("ajay", "2024-09-15"),
        ("vijay", "2024-10-30"),
        ("veer", "2024-10-25"),
        ("aatish", "2024-10-10"),
        ("animesh", "2024-10-15"),
        ("nishad", "2024-11-01"),
        ("varun", "2024-10-05"),
        ("aadil", "2024-11-04")
      ).toDF("name", "last_checkin")

      val status1 = employees.select(initcap(col("name")),
                    when(datediff(current_date(),to_date(col("last_checkin")))<=7,"Active")
                      .otherwise("Inactive").alias("Status"))

      employees.createOrReplaceTempView("employees")

      status1.show()
      val status2 = spark.sql("select initcap(name),CASE WHEN datediff(current_date(),to_date(last_checkin))<=7 THEN 'Active' ELSE 'Inactive' END AS Status from employees")

      status2.show()
    }
}
