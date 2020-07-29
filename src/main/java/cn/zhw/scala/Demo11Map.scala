package cn.zhw.scala

object Demo11Map {
  def main(args: Array[String]): Unit = {
    var map = Map[String, String]()
    // .+ 不会修改原来的 map
    map.+("k" -> "v")
    println(map)

    map += ("k1" -> "v1")
    println(map)

    // 创建时设置值
    var map1 = Map("k" -> "v", "hello" -> "java")
    println(map1)
    println(map1.keys + "\t" + map1.values)

    map1.foreach(x => println(x._1 + "\t" + x._2))

    // 通过 key 获取 value
    println(map1.get("hello").get)
    println(map1("hello"))

    val colors = Map("red" -> "#FF0001", "azure" -> "#F0FFFF", "peru" -> "#CD853F")
    println(colors)

    val colors2 = Map("red" -> "#0033FF", "yellow" -> "#FFFF00", "red" -> "#FF0000")
    println(colors2)

    //map连接
    var colors3 = colors ++ colors2
    println(colors3)
  }
}
