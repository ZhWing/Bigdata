package cn.zhw.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MovieData4_2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("SparkDemo")

    val sc: SparkContext = new SparkContext(conf)

    val data: RDD[String] = sc.textFile("hdfs://192.168.247.137:9000/data/log_movie.txt")

    val data1: RDD[Array[String]] = data.filter(line => line.indexOf("上海堡垒") != (-1)).map(line => line.split(","))

    val data2: RDD[(Long, String, String)] = data1.map(line => {
      (line(4).toLong, line(5), line(7))
    })

    data2.sortBy(-_._1).take(5).foreach(println)
    // data2.saveAsTextFile("hdfs://192.168.247.137:9000/data/log_movie_4_2")
  }
}
