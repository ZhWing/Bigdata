package cn.zhw.spark.day1

import org.apache.spark.{SparkConf, SparkContext}

object SparkFilter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkFilter").setMaster("local")
    val sc = new SparkContext(conf)
    val student = sc.textFile("D:\\兰智数加\\Project\\BigData\\data\\students.txt")

    /**
     * filter 懒执行
     * 返回 true 保留书记
     * 返回 false 过滤数据
     */
    student
      .filter(line => "女".equals(line.split(",")(3)))
      .foreach(println)
  }
}
