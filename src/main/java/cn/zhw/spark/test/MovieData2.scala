package cn.zhw.spark.test

import org.apache.spark.{SparkConf, SparkContext}

object MovieData2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("LogMovie")

    val sc = new SparkContext(conf)

    val data = sc.textFile("hdfs://192.168.247.137:9000/data/log_movie.txt")

    val new_data = data
      .filter(line ⇒ line.indexOf("烈火英雄") != (-1))

    new_data.saveAsTextFile("hdfs://192.168.247.137:9000/data/movie")
  }
}
