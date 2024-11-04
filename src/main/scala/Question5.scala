import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, initcap, sum, when}



object Question5 {

    def main(args:Array[String]): Unit = {
      val spark_conf = new SparkConf()
      spark_conf.set("spark.app.name", "Question4")
      spark_conf.set("spark.master", "local[*]")

      val spark = SparkSession.builder
        .config(spark_conf)
        .getOrCreate()

      import spark.implicits._

      val employees = List(
        ("karthik", 62),
        ("neha", 50),
        ("priya", 30),
        ("mohan", 65),
        ("ajay", 40),
        ("vijay", 47),
        ("veer", 55),
        ("aatish", 30),
        ("animesh", 75),
        ("nishad", 60)
      ).toDF("name", "hours_worked")

      val status1 = employees.select(initcap(col("name")),
        when(col("hours_worked")>60,"Excessive Overtime").
          when(col("hours_worked")>45 && col("hours_worked")<=60, "Standard Overtime")
          .otherwise("No Overtime").alias("Status"))
      status1.show()

    }

}
