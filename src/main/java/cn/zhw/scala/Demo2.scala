package cn.zhw.scala

object Demo2 {
  def main(args: Array[String]): Unit = {
    var a = 10 // 定义变量
    println(a + " " +  a.getClass)

    println(a + 10)
    println(a * 10)
    var str = "Hello World!"
    var list = str.split(" ")
    list.foreach(println)

    val PI = 3.1415926 // 定义一个常量，不可变
    println(PI.getClass)

    if (a > 10) {
      println(a)
    } else {
      println(a + 1)
    }
  }
}
