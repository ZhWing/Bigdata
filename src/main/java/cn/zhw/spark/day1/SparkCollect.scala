package cn.zhw.spark.day1

import org.apache.spark.{SparkConf, SparkContext}

object SparkCollect {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("SparkSample")
    val sc = new SparkContext(conf)
    val student = sc.textFile("D:\\兰智数加\\Project\\BigData\\data\\students.txt")

    /**
     * collect 直接执行
     * 触发 job 任务
     * 讲数据拉去到同一个节点，如果数据量太大，会出现 OOM 内存溢出
     */
    val arr = student.collect()

    arr.foreach(println)
  }
}
