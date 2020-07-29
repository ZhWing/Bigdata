package cn.zhw.scala

/**
 * 特征
 */
object Demo15 {
  def main(args: Array[String]): Unit = {
    val read = new Read()
    read.point("zhangsan")
    read.point(23)
    read.point(List("xiaohei", "xiaohua"))
  }
}


trait Point0 { // 特征，相当于 java 中的抽象类和接口
  /**
   * 抽象方法需要子类实现
   */
  def point(name: String)
  def point(age: Int): Unit ={
    println(age)
  }
}

trait Point1 {
  def point(dogs: List[String]): Unit ={
    println(dogs)
  }
}

class Person01() {

}

class Person02() {

}

class Read extends Person01 with Point0 with Point1 {
  /**
   * 抽象方法需要子类实现
   */
  override def point(name: String): Unit = {
    println(name)
  }
}