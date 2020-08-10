package cn.zhw.spark.test

import org.apache.spark.{SparkConf, SparkContext}

object MovieData4_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("SpakrDemo")

    val sc = new SparkContext(conf)

    val data = sc.textFile("hdfs://192.168.247.137:9000/data/log_movie.txt")

    val data1 = data.filter(line => line.indexOf("上海堡垒") != (-1)).map(line => line.split(","))

    val data2 = data1.map(line => {
      (line(4).toLong, line(5), line(7))
    }).sortBy(-_._1).take(5).foreach(println)
  }
}
