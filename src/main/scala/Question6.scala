import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, initcap, sum, when}

object Question6 {

  def main(args:Array[String]): Unit = {
    val spark_conf = new SparkConf()
    spark_conf.set("spark.app.name","Question6")
    spark_conf.set("spark.master","local[*]")

    val spark = SparkSession.builder()
      .config(spark_conf)
      .getOrCreate()

    import spark.implicits._

    val customers = List(
      ("karthik", 22),
      ("neha", 28),
      ("priya", 40),
      ("mohan", 55),
      ("ajay", 32),
      ("vijay", 18),
      ("veer", 47),
      ("aatish", 38),
      ("animesh", 60),
      ("nishad", 25)
    ).toDF("name", "age")

    val group = customers.select(initcap(col("name")).alias("name"),
      when(col("age")<25,"Youth")
        .when(col("age")>=25 && col("age")<45,"Adult")
        .otherwise("Senior").alias("Group"))
   group.groupBy("Group").agg(count(col("name"))).show()

    customers.createOrReplaceTempView("customers")

    val groupsql = spark.sql("""
      select name,
        case when age <25 then "Youth"
        when age >= 25 and age < 45 then "Adult"
        else "Senior" end as Group
        from customers
      """)
    groupsql.createOrReplaceTempView("groupsql")

    spark.sql(
      """
       select Group,
       count(name)
       from
       groupsql
       group by Group
    """
    ).show()


  }
}
