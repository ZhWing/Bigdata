package cn.zhw.spark.test

import org.apache.spark.{SparkConf, SparkContext}

object MovieData2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("spark demo")
    val sc = new SparkContext(conf);
    // 读取hdfs数据
    val data = sc.textFile("hdfs://192.168.247.137:9000/data/log_movie.txt")
    val data1 = textFileRdd.filter(line => line.indexOf("烈火英雄") != (-1))
    // 写入数据到hdfs系统
    data1.saveAsTextFile("hdfs://192.168.247.137:9000/data/log_movie")
  }
}
