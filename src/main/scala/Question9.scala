
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, initcap, sum, when}


object Question9 {

  def main(args:Array[String]): Unit = {
    val spark_conf = new SparkConf()
    spark_conf.set("spark.app.name", "Question9")
    spark_conf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(spark_conf)
      .getOrCreate()
    import spark.implicits._

    val inventory = List(
      ("ProductA", 120),
      ("ProductB", 95),
      ("ProductC", 45),
      ("ProductD", 200),
      ("ProductE", 75),
      ("ProductF", 30),
      ("ProductG", 85),
      ("ProductH", 100),
      ("ProductI", 60),
      ("ProductJ", 20)
    ).toDF("product_name", "stock_quantity")


    val category = inventory.select(col("product_name"),col("stock_quantity"),
      when(col("stock_quantity")>100,"Overstocked")
        .when(col("stock_quantity")>50 && col("stock_quantity")<=100,"Normal")
        .otherwise("Low Stock").alias("category"))

    category.groupBy("category").agg(sum(col("stock_quantity"))).show()

  }
}
