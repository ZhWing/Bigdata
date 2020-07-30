package cn.zhw.spark.day1

import org.apache.spark.{SparkConf, SparkContext}

object SparkUnion {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("SparkSample")
    val sc = new SparkContext(conf)
//    val student = sc.textFile("D:\\兰智数加\\Project\\BigData\\data\\students.txt")
    val list1 = List(1, 2, 3, 4, 5)
    val list2 = List(6, 7, 8, 9, 10)
    val RDD1 = sc.parallelize(list1)
    val RDD2 = sc.parallelize(list2)

    /**
     * 合并两个 RDD
     * 两个 RDD 的数据要一样
     */
    val unionRDD = RDD1.union(RDD2)
    unionRDD.foreach(println)
  }
}
