package cn.zhw.scala

class Teacher(name: String, age: Int) extends Person(name, age){
  override def getName: String = {
    "子类重写父类方法：" + name
  }

  // 方法重载
  def print(string: String): Unit ={
    println(string)
  }

  def print(int: Int): Unit ={
    println(int)
  }
}
