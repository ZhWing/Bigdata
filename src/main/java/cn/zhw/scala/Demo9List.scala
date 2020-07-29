package cn.zhw.scala

object Demo9List {
  def main(args: Array[String]): Unit = {
    val list = List("java", "Scala", "Python")
    println(list.mkString("\t"))
    println(list.head)
    println(list.tail) // 取出不包含第一个元素的所有元素，返回一个新的列表
    println(list.take(2)) // 从前面取
    println(list.takeRight(2)) // 从后面取
    val list1 = List(1, 2, 3, 4, 10, 5, 6, 7, 8, 9)
    println(list1.takeWhile(x => x % 2 != 0)) // 遇到 false 直接跳过
    println(list1.max)
    println(list1.min)
    println(list1.sum)
    println(list1.reverse) // 反转
    println(list1.sortBy(x => -x)) // 排序 根据某一个字段排序 默认升序

    // 对列表里面元素做一个操作 返回一个新的列表
    list1.map(x => x*2).foreach(println)

    val strings = List("shujia,java,scala", "shujia,java,hadoop", "shujia,java,scala")
    // wordcount
    /**
     * flatMap
     * 1、对没哟个元素做一次 map 返回一个数组
     * 2、把数组里面的所有元素扁平化
     * 3、一行变多行
     */
    strings
      .flatMap(line => line.split(","))
      .map(word => (word, 1)) // 每行打上一个1
      .groupBy(x => x._1) // 以单词分组
      .map(line => (line._1, line._2.map(x => x._2).sum)) // line._1 名称 line._2 计数
      .foreach(println)
  }
}
