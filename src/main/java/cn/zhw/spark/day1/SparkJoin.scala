package cn.zhw.spark.day1

import org.apache.spark.{SparkConf, SparkContext}

object SparkJoin {
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
    val scoreInfoRDD = joinRDD.map(x => {
      val id = x._1
      val studentInfo = x._2._1
      val scoreInfo = x._2._2
      val strings = scoreInfo.split(",")
      studentInfo + ", " + strings(1) + "," + strings(2)
    })

    // 计算学生总分
    scoreInfoRDD
      .map(line => line.split(",").toList)
      // 以学生信息作为 key，分数作为 value
      .map(list => (list.take(5).mkString(","), list(6).toInt))
      .reduceByKey(_ + _)
      .map(x => x._1 + "," + x._2)
      .foreach(println)
  }
}
