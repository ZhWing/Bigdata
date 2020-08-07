package cn.zhw.spark.day1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkReduce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("SparkSample")
    val sc = new SparkContext(conf)

    val list: List[Int] = List(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val RDD: RDD[Int] = sc.parallelize(list)
    val sum: Int = RDD.reduce(_ + _)
    val sum1: Double = RDD.sum()
    println(sum + " " + sum1)
  }
}
