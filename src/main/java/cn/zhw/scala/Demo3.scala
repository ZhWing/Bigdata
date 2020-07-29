package cn.zhw.scala

object Demo3 {
  def main(args: Array[String]): Unit = {
    var array = new Array[String](10)
    array(0) = "Java"
    array(1) = "Scala"

    println(array)
    println(array.mkString(",")) // mkString 把数组拼接成字符串

    // 遍历数组
    for (elem <- array) {
      println(elem)
    }
    // Scala 里面素有序列都有 foreach 函数
    array.foreach(println)

    // x是函数的参数
    // x => println(x) 匿名函数
    array.foreach(x => println(x))

    // 直接创建数组
    var a = Array(1, 2, 3, 4)
    println(a.min + " " + a.max + " " + a.length + " " + a.head)
    println(a.mkString(", "))

    var arr = Array.ofDim[Int](3, 4);
    for (elem <- arr) {
      println(elem.mkString(", "))
    }

    def p(int: Int) : Unit = {
      println(int)
    }
    /*
     * 高阶函数
     * 1、以函数作为参数
     * 2、以函数作为返回值
     */
    a.foreach(p)

    println("=" * 10)

    var r = Range(1, 10)
    println(r)
    for (elem <- r) {
      print(elem + " ")
    }
    print("\n")
    r.foreach(i => print(i + " "))
  }
}