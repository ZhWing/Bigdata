package cn.zhw.scala

object Demo5 {
  def main(args: Array[String]): Unit = {
    val student = new Student("张三", 18)

    // println 和 java 里面的System.out.println 效果和底层实现是一样的
    println(student)
    System.out.println(student)

    val person = new Person("李四", 24)

    val teacher = new Teacher("小伟", 27)

    println(teacher.getName  + " " + teacher.getAge)
    teacher.print(11)
    teacher.print("qqq")
  }

}
