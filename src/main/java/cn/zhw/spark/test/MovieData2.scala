package cn.zhw.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MovieData2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("spark demo")
    val sc: SparkContext = new SparkContext(conf)
    // 读取hdfs数据
    val data: RDD[String] = sc.textFile("hdfs://192.168.247.137:9000/data/log_movie.txt")
    val data1: RDD[String] = data.filter(line => line.indexOf("烈火英雄") != (-1))
    // 写入数据到hdfs系统
    data1.saveAsTextFile("hdfs://192.168.247.137:9000/data/log_movie_2")
  }
}
