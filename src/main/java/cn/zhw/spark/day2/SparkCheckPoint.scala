package cn.zhw.spark.day2

import org.apache.spark.{SparkConf, SparkContext}

object SparkCheckPoint {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("SparkCheckPoint")

    val sc = new SparkContext(conf)

    var studentRDD = sc
      .textFile("data/temp.txt")
      .map(lines ⇒ lines.split(","))

    /**
     * 缓存到 task 运行节点的内存中
     */
    studentRDD = studentRDD.cache()

    /**
     * 将 RDD 里面的数据存到 hdfs
     * 切断 RDD 之间的联系
     * 后面对这个 RDD 的操作将基于 hdfs 展开
     */
    // 需要设置 CheckPoint 路径
    sc.setCheckpointDir("data/checkpoint")
    studentRDD.checkpoint()

    studentRDD.foreach(println)
  }
}
