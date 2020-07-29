package cn.zhw.scala

object Demo12String {
  def main(args: Array[String]): Unit = {
    val s ="shujia"
    val f = 3.14
    printf("String: %s Float: %f", s, f)
    println()

    val stringBuilder = new StringBuilder
    // .++ 追加字符串
    stringBuilder.++=("shujia")
    stringBuilder.+= ('A')
    stringBuilder ++="Hello"
    println(stringBuilder)

    var ss = "shujia"
    println(ss.charAt(0))

    val copyS = String.copyValueOf(Array('h', 'e', 'l', 'l', 'o'), 2, 3)
    println(copyS)
  }
}
