package cn.zhw.spark.day1

import org.apache.spark.{SparkConf, SparkContext}

object SparkSample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("SparkSample")
    val sc = new SparkContext(conf)
    val student = sc.textFile("D:\\兰智数加\\Project\\BigData\\data\\students.txt")

    /**
     * sample 懒执行
     * withReplacement：是否放回
     * fraction：抽样比例
     */
    val sampleRDD = student.sample(true, 0.1)
    println(sampleRDD.count())
  }
}
