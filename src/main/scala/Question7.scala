import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, initcap, sum, when}



object Question7 {

  def main(args:Array[String]): Unit = {

    val spark_conf = new SparkConf()
    spark_conf.set("spark.app.name","Question7")
    spark_conf.set("spark.master","local[*]")

    val spark = SparkSession.builder()
      .config(spark_conf)
      .getOrCreate()

import spark.implicits._

    val vehicles = List(
      ("CarA", 30),
      ("CarB", 22),
      ("CarC", 18),
      ("CarD", 15),
      ("CarE", 10),
      ("CarF", 28),
      ("CarG", 12),
      ("CarH", 35),
      ("CarI", 25),
      ("CarJ", 16)
    ).toDF("vehicle_name", "mileage")

    val milegegroup = vehicles.select(col("vehicle_name"),
      when(col("mileage")<25,"High Efficiency")
        .when(col("mileage")>=15 && col("mileage")<25,"Moderate Efficiency")
        .otherwise("Low Efficiency").alias("MileageGroup"))

    milegegroup.show()

  }

}
