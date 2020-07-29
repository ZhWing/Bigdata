package cn.zhw.scala

import scala.io.Source
import java.io.File

object Demo14Implicit3 {
  def main(args: Array[String]): Unit = {
    val file = new File("data\\students.txt")
    val source = Source.fromFile(file)
    val lines: Iterator[String] = source.getLines()
    lines.foreach(println)

    new FileRead(file).read().foreach(println)

    // implicit def cat(file: File) = new FileRead(file)

    /**
     * 隐式转换，在不改变原来类的源码的情况下，动态增加新的方法
     */
    val str = file.read()
    str.size
  }

  /**
   * 3、隐式转换类
   */
  implicit class FileRead(file: File) {
    def read() = Source.fromFile(file).getLines().toList
  }
}
