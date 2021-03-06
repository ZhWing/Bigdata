package cn.zhw.spark.test2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object demo4 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("SparkDemo")
    conf.setMaster("local")

    val sc: SparkContext = new SparkContext(conf)
    val data: RDD[String] = sc.textFile("data/ershoufang-clean-utf8-v1.1.csv")

    val data1: RDD[String] = data.filter(line => line.split(",")(2) != "" && line.split(",")(4) != "")
    val data2: RDD[(String, Long)] = data1.map(line => (line.split(",")(2), line.split(",")(4).toLong))
    val data3: RDD[(String, Double)] = data2.groupByKey().map(line => (line._1, (line._2.sum.toDouble / line._2.toArray.length)))
    data3.sortBy(line => -line._2).foreach(println)
  }
}