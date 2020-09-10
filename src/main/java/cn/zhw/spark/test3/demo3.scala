package cn.zhw.spark.test3

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object demo3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("SparkDemo")
    conf.setMaster("local")

    val sc: SparkContext = new SparkContext(conf)
    
    val data: RDD[String] = sc.textFile("data/top250_f1.txt")
    
    val data1: RDD[String] = data.filter(line => line.split("\t")(6).indexOf("爱情") != (-1) && line.split("\t")(6).indexOf("剧情") != (-1))
    
    val data2: RDD[(String, Double)] = data1.map(line => (line.split("\t")(1), (line.split("\t")(7).toDouble) * (line.split("\t")(8).toDouble)))
    
    data2.sortBy(line => -line._2).take(10).foreach(println)
  }
}