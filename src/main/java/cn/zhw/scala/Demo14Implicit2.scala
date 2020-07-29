package cn.zhw.scala

object Demo14Implicit2 {
  def main(args: Array[String]): Unit = {
    Point("张三")("fun")

    /**
     * 2、隐式转换变量
     */
    // 同一个作用域里面不能有两个类型相同的隐式转换变量
    implicit val s: String = "implicit"
    // implicit val s1: String = "implicit1"
    Point("张三")
  }

  /**
   * 柯里化
   */
  def Point(name: String)(implicit prefix: String): Unit ={
    println(prefix + " " + name)
  }
}
