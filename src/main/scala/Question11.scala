import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, initcap, sum, when}

object Question11 {
  def main(args:Array[String]): Unit = {
    val spark_conf = new SparkConf()
    spark_conf.set("spark.app.name","Question11")
    spark_conf.set("spark.master","local[*]")

    val spark = SparkSession.builder()
      .config(spark_conf)
      .getOrCreate()
import spark.implicits._
    val products = List(
      ("Laptop", "Electronics", 120, 45),
      ("Smartphone", "Electronics", 80, 60),
      ("Tablet", "Electronics", 50, 72),
      ("Headphones", "Accessories", 110, 47),
      ("Shoes", "Clothing", 90, 55),
      ("Jacket", "Clothing", 30, 80),
      ("TV", "Electronics", 150, 40),
      ("Watch", "Accessories", 60, 65),
      ("Pants", "Clothing", 25, 75),
      ("Camera", "Electronics", 95, 58)
    ).toDF("product_name", "category", "return_count", "satisfaction_score")

    val category = products.select(col("product_name"),col("category"),
      when(col("return_count")>100 && col("satisfaction_score") < 50 ,"High Return Rate")
        .when(col("return_count").between(50,100) && col("satisfaction_score").between (50,70) ,"Moderate Return Rate")
        .otherwise("Low Return Rate").alias("return_reason"))

    category.groupBy("return_reason","category").agg(count(col("product_name"))).orderBy("return_reason","category").show()

    products.createOrReplaceTempView("products")

    val categorysql = spark.sql("""
      select product_name, category,
        case when return_count >100 AND satisfaction_score < 50 then "High Return Rate"
        when return_count between 50 and 100 and satisfaction_score between 50 and 70 then "Moderate Return Rate"
        else "Low Return Rate" end as return_reason
        from products
      """)
    categorysql.createOrReplaceTempView("categorysql")

    spark.sql(
      """
       select return_reason,category,
       count(product_name)
       from
       categorysql
       group by return_reason,category
    """
    ).show()

  }
}
