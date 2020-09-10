package cn.zhw.spark.test3

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object demo2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("SparkDemo")
    conf.setMaster("local")

    val sc: SparkContext = new SparkContext(conf)

    val data: RDD[String] = sc.textFile("data/top250_f1.txt")
    val data2: RDD[String] = data.filter(line => line.split("\t")(4).toLong > 2013)
    
    val data3: RDD[(String, Int)] = data2.map(line => (line.split("\t")(5), 1))
    
    data3.reduceByKey(_+_).sortBy(line => -line._2).take(5).foreach(println)
  }
}