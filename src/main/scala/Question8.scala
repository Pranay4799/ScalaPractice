import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, initcap, sum, when}



object Question8 {
  def main(args:Array[String]): Unit = {
    val spark_conf = new SparkConf()
    spark_conf.set("spark.app.name", "Question8")
    spark_conf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(spark_conf)
      .getOrCreate()
    import spark.implicits._
    val students = List(
      ("karthik", 95),
      ("neha", 82),
      ("priya", 74),
      ("mohan", 91),
      ("ajay", 67),
      ("vijay", 80),
      ("veer", 85),
      ("aatish", 72),
      ("animesh", 90),
      ("nishad", 60)
    ).toDF("name", "score")

    val category = students.select(col("name"),
      when(col("score")>90,"Excellent")
        .when(col("score")>75 && col("score")<89,"Good")
        .otherwise("Needs Improvement").alias("category"))

    category.groupBy("category").agg(count(col("name"))).show()

    students.createOrReplaceTempView("students")

    val categorysql = spark.sql("""
      select name,
        case when score >90 then "Excellent"
        when score >75 and score < 89 then "Good"
        else "Needs Improvement" end as category
        from students
      """)
    categorysql.createOrReplaceTempView("categorysql")

    spark.sql(
      """
       select category,
       count(name)
       from
       categorysql
       group by category
    """
    ).show()


  }
}
