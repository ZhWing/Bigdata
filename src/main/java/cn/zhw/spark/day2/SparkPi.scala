package cn.zhw.spark.day2

import org.apache.commons.math3.util.FastMath.random
import org.apache.spark.{SparkConf, SparkContext}

object SparkPi {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("SparkPi")
    val context = new SparkContext(conf)
    // 1 to 1000 : 1到 1000 的序列
    val list = 1 until 1000000000 // 1到 999 的序列
    val count = context.parallelize(list, 10)
      .map { i =>
        val x = random * 2 - 1
        val y = random * 2 - 1
        if (x * x + y * y < 1) 1 else 0
      }.reduce(_ + _)
    println("Pi is Roughly " + 4.0 * count / 1000000000)
    context.stop()
  }
}
