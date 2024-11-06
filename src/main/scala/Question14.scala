
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, collect_list, count, initcap, struct, sum, when}
import org.apache.spark.sql.functions._

object Question14 {
  def main(args:Array[String]): Unit = {
   val spark_conf = new SparkConf()
    spark_conf.set("spark.app.name","Question14")
    spark_conf.set("spark.master","local[*]")

    val spark= SparkSession.builder()
      .config(spark_conf)
      .getOrCreate()

    import spark.implicits._

    val loanApplicants = List(
      ("karthik", 60000, 120000, 590),
      ("neha", 90000, 180000, 610),
      ("priya", 50000, 75000, 680),
      ("mohan", 120000, 240000, 560),
      ("ajay", 45000, 60000, 620),
      ("vijay", 100000, 100000, 700),
      ("veer", 30000, 90000, 580),
      ("aatish", 85000, 85000, 710),
      ("animesh", 50000, 100000, 650),
      ("nishad", 75000, 200000, 540)
    ).toDF("name", "income", "loan_amount", "credit_score")



    val risk_level = loanApplicants.select(col("name"), when(col("loan_amount") >= col("income")*2 && col("credit_score") < 600, "High Risk")
      .when(col("loan_amount").between(col("income") *1,  col("income")*2) && col("credit_score").between(600, 700), "Moderate Risk")
      .otherwise("Low Risk").alias("risk_level"))

    risk_level.groupBy("risk_level").agg(count(col("name"))).show()

  }
}
