package cn.zhw.spark.test3

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object demo1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("SparkDemo")
    conf.setMaster("local")

    val sc: SparkContext = new SparkContext(conf)

    val data: RDD[String] = sc.textFile("data/top250_f1.txt")
    val data1: RDD[(String, Long)] = data.map(line => (line.split("\t")(1), line.split("\t")(8).toLong))
    data1.reduceByKey(_+_).sortBy(line => -line._2).take(5).foreach(println)
  }
}