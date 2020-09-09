package cn.zhw.spark.test2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object demo2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("SparkDemo")
    conf.setMaster("local")

    val sc: SparkContext = new SparkContext(conf)
    val data: RDD[String] = sc.textFile("data/ershoufang-clean-utf8-v1.1.csv")

    val data2: RDD[String] = data.filter(line => "普通住宅".equals(line.split(",")(20)))
    val cx: Long = data2.count()
    val data3: RDD[(String, Int)] = data2.map(line => (line.split(",")(2), 1))
    // val data4: RDD[(String, Int)] = data3.groupByKey().map(x => (x._1, x._2.sum))
    val data4: RDD[(String, Int)] = data3.reduceByKey(_ + _)
    data4.map(line => (line._1, (line._2.toDouble * 100 / cx).formatted("%.2f") + "%")).foreach(println)
  }
}