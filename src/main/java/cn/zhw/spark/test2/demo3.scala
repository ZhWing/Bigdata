package cn.zhw.spark.test2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object demo3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SparkDemo")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val data: RDD[String] = sc.textFile("data/ershoufang-clean-utf8-v1.1.csv")
    val data1: RDD[String] = data.filter(line => line.split(",")(2) != "" && line.split(",")(2) != "areaName")

    val data2: RDD[(String, Int)] = data1.map(line => (line.split(",")(2), 1))
    data2.reduceByKey(_+_).sortBy(line => line._2).foreach(println)
  }
}