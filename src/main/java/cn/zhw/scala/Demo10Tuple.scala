package cn.zhw.scala

object Demo10Tuple {
  def main(args: Array[String]): Unit = {
    var t = (1, 2, 3, 4)
    println(t._1)

    var t2 = ("java", 30)
    println(t2._1 + "\t" + t2._2)
    println(t2.swap)

    def show(opt: Option[String]) = opt match {
      case Some(s) => s
      case None => "?"
    }
    println(show(None))

    def show1(opt : String) = opt match {
      case "java" => "hello"
      case "spark" => "scala"
    }
    println(show1("spark"))
  }
}
