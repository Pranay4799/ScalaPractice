import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, dayofmonth}

object S3Q1 {

  def main(args:Array[String]):Unit= {

    val spark_conf = new SparkConf()
    spark_conf.set("spark.app.name","S3Q1")
    spark_conf.set("spark.master","local[*]")

    val spark = SparkSession.builder()
      .config(spark_conf)
      .getOrCreate()

    import spark.implicits._
    val d1 = Seq(("2024-01-15"), ("2024-02-20"), ("2024-03-25")).toDF("date")
    //dayofmonth
    d1.withColumn("month",dayofmonth(col("date"))).show()



  }
}
