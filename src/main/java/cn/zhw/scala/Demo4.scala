package cn.zhw.scala

/**
 * 高阶函数
 * 1、以函数作为参数
 * 2、以函数作为返回值
 */

object Demo4 {
  def main(args: Array[String]): Unit = {

    // 返回值类型可以省略
    def fun1(a: Int): String = {
      println("没有返回值的函数:" + a)
      "返回值" // return 可以省略，默认以最后一行作为返回值
    }

    val str = fun1(1)
    println(str)

    /**
     * 以函数作为参数
     * 函数的类型通过函数参数的类型和函数返回值的类型确定
     */
    /**
     * 调用这个函数需要传入一个参数为 Int，返回值为 String 的函数
     */
    def fun2(a: Int => String) = {
      a(12)
    }

    def arg(a: Int): String = {
      a.toString
    }
    println(fun2(arg))


    /**
     * 以函数作为返回值
     */
    def fun3(): Int => String = {
      def a(b: Int): String = {
        b.toString
      }
      return a
    }

    // 返回一个函数
    val intToString = fun3()
    // 调用返回的函数
    val str1 = intToString(23)
    println(str1)

    // 第一个括号对fun3调用，第二个括号对返回的函数调用
    println(fun3()(24))

    /**
     * 以函数作为返回值
     */
    def fun4(a: String): String => String ={
      def fun5(b: String): String = {
        return a + ":函数作为参数:" + b
      }
      return fun5
    }
    val str2 = fun4("Scala")("Java")
    println(str2)

    // 简写
    def fun6(a: String)(b: String): String ={
      return a + ":函数作为参数:" + b
    }
    println(fun6("a")("b"))

    /**
     * 偏应用函数
     */
     def mov(x: Int, y: Int): Unit ={
       println("横坐标：" + x)
       println("纵坐标：" + y)
     }
    mov(1, 2)
    val f = mov(10: Int, _: Int)
    f(20)
    f(30)

    /**
     * 可变参数
     * 可变参数只能放在参数的最后
     */
    def fun7(age: Int, name: String*): Unit ={
      println(age)
      name.foreach(println)
    }
    fun7(18, "a", "b", "c")
  }
}
