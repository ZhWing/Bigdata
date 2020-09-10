package cn.zhw.spark.test2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object demo1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("SparkDemo")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val data: RDD[String] = sc.textFile("data/ershoufang-clean-utf8-v1.1.csv")

    val data2: RDD[String] = data.filter(line => "商品房".equals(line.split(",")(18)) && "普通住宅".equals(line.split(",")(20)))
    println(data2.count())
    data2.saveAsTextFile("data/ershoufang")
  }
}