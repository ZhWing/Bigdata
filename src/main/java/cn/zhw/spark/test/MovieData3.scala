package cn.zhw.spark.test

import org.apache.spark.{SparkConf, SparkContext}

object MovieData3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("SparkDemo")

    val sc = new SparkContext(conf)

    val data = sc.textFile("hdfs://192.168.247.137:9000/data/log_movie/part-00000")

    val data1 = data.map(line => (line.split(",")(8), 1))
    data1.foreach(println)

    val data2 = data1.reduceByKey(_ + _)
    data2.foreach(println)
    // data2.saveAsTextFile("hdfs://192.168.247.137:9000/data/log_movie_1")
  }
}
