package cn.zhw.spark.day1

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    /**
     * 1、创建 spark 配置文件对象
     */
    val conf = new SparkConf()
    // 指定 spark 运行模式
    // local 本地模式
    conf.setMaster("local")
    // 任务名称
    conf.setAppName("WordCount")

    /**
     * 2、创建 spark 上下文对象
     */
    val context = new SparkContext(conf)

    /**
     * 3、通过上下文对象读取数据，构建 RDD
     */
    val RDD1 = context.textFile("data/word.txt")

    /**
     * foreach action 类算子，触发任务执行
     * 如果没有 foreach action 类算子，整个任务不会执行
     */
    RDD1.foreach(println)
    println(RDD1.count())

    /**
     * WordCount
     * 1、扁平化
     * 2、每一个增加一列 1
     * 3、分组聚合
     */
    val RDD2 = RDD1.flatMap(line => line.split(","))
    val RDD3 = RDD2.map(word => (word, 1))

    /**
     * reduceByKey 先分组在聚合
     */
    val RDD4 = RDD3.reduceByKey((x, y) => x + y)
    // RDD3.groupByKey().map(x => (x._1, x._2.sum)).foreach(println)
    RDD4.foreach(x => println(x._1 + "\t" + x._2))
    while (true) {

    }
  }
}
