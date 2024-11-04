import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, collect_list, count, initcap, struct, sum, when}

object Question13 {
  def main(args:Array[String]): Unit = {
    val spark_conf = new SparkConf()
    spark_conf.set("spark.app.name", "Question13")
    spark_conf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(spark_conf)
      .getOrCreate()
    import spark.implicits._

    val orders = List(
      ("Order1", "Laptop", "Domestic", 2),
      ("Order2", "Shoes", "International", 8),
      ("Order3", "Smartphone", "Domestic", 3),
      ("Order4", "Tablet", "International", 5),
      ("Order5", "Watch", "Domestic", 7),
      ("Order6", "Headphones", "International", 10),
      ("Order7", "Camera", "Domestic", 1),
      ("Order8", "Shoes", "International", 9),
      ("Order9", "Laptop", "Domestic", 6),
      ("Order10", "Tablet", "International", 4)
    ).toDF("order_id", "product_type", "origin", "delivery_days")

    val category = orders.select(col("order_id"),
      col("product_type"),
      when(col("delivery_days") > 7 && col("origin") === "International", "Delayed")
        .when(col("delivery_days").between(3, 7), "On-Time")
        .otherwise("Fast").alias("speed_category")
    )
    category.groupBy("product_type", "speed_category").agg(count(col("order_id"))).show()

    orders.createOrReplaceTempView("orders")

    val categorysql = spark.sql("""
      select order_id, product_type,
        case when delivery_days >7 AND origin = "International" then "Delayed"
        when delivery_days between 3 and 7 then "On-Time"
        else "Fast" end as speed_category
        from orders
      """)
    categorysql.createOrReplaceTempView("categorysql")

    spark.sql(
      """
       select product_type,speed_category,count(order_id)
       from categorysql group by product_type,speed_category
    """
    ).show()
  }
}
