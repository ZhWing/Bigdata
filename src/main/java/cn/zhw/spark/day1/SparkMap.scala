package cn.zhw.spark.day1

import org.apache.spark.{SparkConf, SparkContext}

object SparkMap {
  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3, 4, 5, 6)
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("SparkMap")

    val sc = new SparkContext(conf)

    /**
     * sc.textFile("")
     * spark 本上没有读取文件的方法，用的还是 mapreduce 读取文件的方法
     * local 模式：读取本地文件
     * yarn 模式：读取 hdfs
     */

    /**
     * 把 Scala 集合序列化成一个 RDD
     */
    val listRDD = sc.parallelize(list)

    /**
     * map 传入一个对象返回一个对象
     * 懒执行
     */
    val RDD2 = listRDD.map(x => x * x)

    RDD2.foreach(println)

    val student = sc.textFile("D:\\兰智数加\\Project\\BigData\\data\\students.txt")
    student
      .map(line => line.split(","))
      .map(arr => Student(arr(0), arr(1), arr(2).toInt, arr(3), arr(4)))
      .map(s => s.name).foreach(println)
  }

}

case class Student(id: String, name: String, age: Int, gender: String, clazz: String)
