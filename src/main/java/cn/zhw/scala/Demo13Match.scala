package cn.zhw.scala

object Demo13Match {
  def main(args: Array[String]): Unit = {

    def isNum(int: Int) = int match {
      case 1 => println("one")
      case 2 => println("two")
      case _ => println("no")
    }
    isNum(13)

    def matchDog(dog: Dog) = dog match {
      case Dog("小黑", 12) => println("小黑")
      case dog: Dog => println("dog")
      case _ => println("No!")
    }

    val dog = Dog("小黑", 13)
    matchDog(dog)

    // 过滤出小黄和小黑
    val dogs = List(Dog("小黑", 12), Dog("小白", 13), Dog("小黄", 14), Dog("小花", 15))
    dogs.filter(dog => dog match {
      case Dog("小黄", 14) => true
      case Dog("小黑", 12) => true
      case _ => false
    }).foreach(println)

    // 模式匹配
    val map = Map("1500100003" -> "单乐蕊", "1500100004" -> "葛德曜", "1500100005" -> "宣谷芹")
    val sname = map.get("1500100000") match {
      case Some(name) => name
      case None => "No"
    }
    println(sname)
  }
}

case class Dog(name: String, age: Int)
