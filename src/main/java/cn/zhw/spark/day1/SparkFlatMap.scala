package cn.zhw.spark.day1

import org.apache.spark.{SparkConf, SparkContext}

object SparkFlatMap {
  def main(args: Array[String]): Unit = {
    val list = List("java,scala,python", "java,spark,hadoop")

    val conf = new SparkConf().setAppName("SparkFlatMap").setMaster("local")
    val sc = new SparkContext(conf)

    val RDD1 = sc.parallelize(list)

    /**
     * flatMap 懒执行
     * 传入一个对象，返回一个序列
     * 1、map 操作
     * 2、对返回的数据做扁平化操作
     */
    RDD1
      .flatMap(line => line.split(","))
      .foreach(println)
  }
}
