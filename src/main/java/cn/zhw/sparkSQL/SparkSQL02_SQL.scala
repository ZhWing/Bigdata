package cn.zhw.sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL02_SQL {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val frame: DataFrame = spark.read.json("data/test.json")

    frame.createOrReplaceTempView("user")

    // 采用 SQL 的语法访问数据
    spark.sql("select * from user").show

    // 释放资源
    spark.stop()
  }
}
