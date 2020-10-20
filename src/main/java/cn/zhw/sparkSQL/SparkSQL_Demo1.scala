package cn.zhw.sparkSQL

import org.apache.spark.sql.SparkSession

object SparkSQL_Demo1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SparkSQL")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    spark.read
      .option("sep", ",")
      .schema("id string, name string, age int, sex string, clazz string") // 1500100001,施笑槐,22,女,文科六班
      .csv("data/students.txt")
      .createOrReplaceTempView("students")

    spark.sql("select * from students").show()
  }
}
