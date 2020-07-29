package cn.zhw.scala

import java.io.{BufferedReader, FileReader}

import scala.io.{BufferedSource, Source}

object Demo8 {
  def main(args: Array[String]): Unit = {
//    val reader = new BufferedReader(
//      new FileReader("data\\students.txt"))
//    var line = reader.readLine()
//    while (line != null) {
//      println(line)
//      line = reader.readLine()
//    }
    /**
     * Scala 提供读取数据的方式
     */
    val source = Source.fromFile("data\\students.txt").getLines()
    val list = source.toList
    // 根据年龄排序
    list.sortBy(line => line.split(",")(2).toInt).foreach(println)

    // 字典排序
    list.sortWith((x, y) => {
      val str1 = x.split(",")(4)
      val str2 = y.split(",")(4)
      str1.compareTo(str2) > 0
    }).foreach(println)

    // 取出数据里的某一列
    list.map(line => line.split(",")(1)).foreach(println)

    // 统计班级人数
    val list1 = list
      .map(line => line.split(",")(4))
      .map(word => (word, 1)) // 每行打上一个1
      .groupBy(x => x._1) // 以班级分组
      .map(line => (line._1, line._2.map(x => x._2).sum)) // line._1 班级 line._2 计数

    for (elem <- list1) {
      println(elem._1 + ", " + elem._2)
    }

    // 键盘录入
    val str = Console.in.readLine()
    println(str)
  }
}
