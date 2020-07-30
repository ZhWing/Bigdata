package cn.zhw.spark.day1

import org.apache.spark.{SparkConf, SparkContext}

object Spark_ByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("SparkSample")
    val sc = new SparkContext(conf)
    val student = sc.textFile("D:\\兰智数加\\Project\\BigData\\data\\students.txt")

    /**
     * 统计每个班级学生人数
     *
     */
    val tupleRDD = student
      .map(line => line.split(",")(4)) // 取出班级列
      .map(line => (line, 1))

      /**
       * groupByKey() 懒执行
       * 必须作用在 KV 格式的 RDD 上
       * 默认是 hash 分区
       */
      tupleRDD.groupByKey()
      .map(x => (x._1, x._2.sum))
      .foreach(println)

    // tupleRDD.reduceByKey((x, y) => x + y)
    tupleRDD.reduceByKey(_ + _).foreach(println)
  }
}
