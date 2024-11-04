import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, collect_list, count, initcap, struct, sum, when}

object Question12 {
  def main(args:Array[String]): Unit = {
    val spark_conf = new SparkConf()
    spark_conf.set("spark.app.name","Question12")
    spark_conf.set("spark.master","local[*]")

    val spark = SparkSession.builder()
      .config(spark_conf)
      .getOrCreate()
    import spark.implicits._

    val customers = List(
      ("karthik", "Premium", 1050, 32),
      ("neha", "Standard", 800, 28),
      ("priya", "Premium", 1200, 40),
      ("mohan", "Basic", 300, 35),
      ("ajay", "Standard", 700, 25),
      ("vijay", "Premium", 500, 45),
      ("veer", "Basic", 450, 33),
      ("aatish", "Standard", 600, 29),
      ("animesh", "Premium", 1500, 60),
      ("nishad", "Basic", 200, 21)
    ).toDF("name", "membership", "spending", "age")

    val category = customers.select(col("name"),
      col("membership"),
      col("spending"),
      when(col("spending") > 1000 && col("membership") === "Premium", "High Spender")
        .when(col("spending").between(500, 1000) && col("membership") === "Standard", "Average Spender")
        .otherwise("Low Spender").alias("spending_category")
    )
    category.groupBy("membership").agg(avg(col("spending"))).show()

    customers.createOrReplaceTempView("customers")

    val categorysql = spark.sql("""
      select name, membership,spending,
        case when spending >1000 AND membership = "Premium" then "High Spender"
        when spending between 500 and 1000 and membership ="Standard" then "Average Spender"
        else "Low Spender" end as spending_category
        from customers
      """)
    categorysql.createOrReplaceTempView("categorysql")

    spark.sql(
      """
       select membership,
       avg(spending)
       from
       categorysql
       group by membership
    """
    ).show()
  }
}
