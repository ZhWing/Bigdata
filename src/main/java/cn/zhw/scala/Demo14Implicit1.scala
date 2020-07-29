package cn.zhw.scala

// 隐式转换
object Demo14Implicit1 {
  def main(args: Array[String]): Unit = {
    val str = "I love Java"
    str.foreach(println)

    def Print(s: String) ={
      println(s)
    }

    Print("I love Java")

    /**
     * 隐式转换函数
     * 动态再当前作用域起作用
     * 在当前作用域里面寻找可用的隐式转换方法
     */
    /**
     * 1、隐式转换函数
     */
    implicit def intToString(i: Int): String = i.toString
    implicit def doubleToString(d: Double): String = d.toString

    Print(12)
    Print(3.14)
    Print(11.toString) // 显式转换
  }
}
