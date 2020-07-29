package cn.zhw.scala

import java.io.PrintWriter
import scala.io.Source

object WordCount {
  def main(args: Array[String]): Unit = {

    val out = new PrintWriter("data/temp.txt")

    Source
      .fromFile("data/word.txt") // 1、读取文件
      .getLines() // 2、获取所有数据
      // .flatMap(line => line.split(","))
      // '_' 代表的是第一个参数
      // '_' 只能用一次
      .flatMap(_.split(",")) // 3、扁平化
      // .foreach(println(_))
      .map((_, 1))
      .toList // 迭代器不能分组
      .groupBy(_._1) // 根据单词分组
      .map(x => (x._1, x._2.map(_._2).sum)) // 组内求和
      .toList
      .sortBy(line => -line._2.toInt) // 根据次数排序
      .foreach(line => {
        out.println(line._1 + "\t" + line._2)
      })

    out.close()
  }
}
