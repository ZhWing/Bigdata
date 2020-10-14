package cn.zhw.sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SpaekSQL01_Demo {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("SparkSQL")

    // 构建 SparkSQL 的环境对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    // 读取数据 构建 DataFrame
    val frame: DataFrame = spark.read.json("data/test.json")

    // 展示数据
    frame.show()

    // 释放资源
    spark.stop()
  }
}
