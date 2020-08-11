package cn.zhw.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object MovieData4_1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("SparkDemo")

    val sc: SparkContext = new SparkContext(conf)

    val data: RDD[String] = sc.textFile("hdfs://192.168.247.146:9000/data/log_movie.txt")
    val data1: RDD[Array[String]] = data.filter(line => line.indexOf("上海堡垒") != (-1)).map(line => line.split(",")) // .foreach(line => println(line(1)))

    val data2: RDD[(String, Int)] = data1.map(line => {
      if (line(1) .indexOf("2019-昨天") == (-1)){
        (line(1), 1)
      } else {
        ("2019-昨天", 1)
      }
    }) // .foreach(println)

    data2.reduceByKey(_ + _).sortBy(line => line._1).foreach(println)
    data2.saveAsTextFile("hdfs://192.168.247.146:9000/data/log_movie_4_1")
  }
}
