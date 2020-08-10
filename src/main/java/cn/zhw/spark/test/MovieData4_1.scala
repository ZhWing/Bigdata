package cn.zhw.spark.test

import org.apache.spark.{SparkConf, SparkContext}
object MovieData4_1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("SpakrDemo")

    val sc = new SparkContext(conf)

    val data = sc.textFile("hdfs://192.168.247.137:9000/data/log_movie.txt")
    val data1 = data.filter(line => line.indexOf("上海堡垒") != (-1)).map(line => line.split(",")) // .foreach(line => println(line(1)))

    val data2 = data1.map(line => {
      if (line(1) .indexOf("2019-昨天") == (-1)){
        (line(1), 1)
      } else {
        ("2019-昨天", 1)
      }
    }) // .foreach(println)

    data2.reduceByKey(_ + _).sortBy(line => line._1).foreach(println)
  }
}
