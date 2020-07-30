package cn.zhw.spark.day1

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object SparkSave {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("SparkSample")
    val sc = new SparkContext(conf)

    val students = sc.textFile("D:\\兰智数加\\Project\\BigData\\data\\students.txt")
    val scores = sc.textFile("D:\\兰智数加\\Project\\BigData\\data\\score.txt")
    val studentsRDD = students.map(line => (line.split(",")(0), line))
    val scoreRDD = scores.map(line => (line.split(",")(0), line))
    /**
     * 连接两个 RDD
     */
    val joinRDD = studentsRDD.join(scoreRDD)
    var scoreInfoRDD = joinRDD.map(x => {
      val id = x._1
      val studentInfo = x._2._1
      val scoreInfo = x._2._2
      val strings = scoreInfo.split(",")
      studentInfo + ", " + strings(1) + "," + strings(2)
    })

    // 对多次是用的 RDD 进行缓存
    // 这个时候 scoreInfoRDD 里面就有数据了
    // cache 数据默认缓存到内存

    /**
     * 缓存
     * 同一个 RDD 被多次使用
     * 如果没有缓存会被多次计算，效率较低
     * 使用缓存能提高执行效率
     *
     * 使用缓存回占用内存
     * 如果数据量较大，会出现 OMM 内存溢出
     *
     * cache 使用的是 MEMORY_ONLY
     * MEMORY_ONLY
     * MEMORY_ONLY_SER
     * MEMORY_AND_DISK_SER
     */
    // scoreInfoRDD = scoreInfoRDD.cache()
    scoreInfoRDD = scoreInfoRDD.persist(StorageLevel.MEMORY_ONLY)
    // 内存不够放磁盘上
    scoreInfoRDD = scoreInfoRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)

    // 计算学生总分
    val sumCoreRDD = scoreInfoRDD
      .map(line => line.split(",").toList)
      // 以学生信息作为 key，分数作为 value
      .map(list => (list.take(5).mkString(","), list(6).toInt))
      .reduceByKey(_ + _)
      .map(x => x._1 + "," + x._2)

    // 保存数据到磁盘

    /**
     * saveAsTextFile 触发 job 任务
     * 实际上也是使用的 mapreduce 写磁盘的方法
     */
    sumCoreRDD.saveAsTextFile("D:\\兰智数加\\Project\\BigData\\data\\out")

    val avgScoreRDD = scoreInfoRDD
      .map(line => line.split(",").toList)
      // 以学生信息作为 key，分数作为 value
      .map(list => (list.take(5).mkString(","), list(6).toInt))
      .groupByKey()
      .map(x => (x._1, x._2.sum / x._2.count(x => true).toDouble))

    avgScoreRDD.saveAsTextFile("D:\\兰智数加\\Project\\BigData\\data\\out1")
  }
}
