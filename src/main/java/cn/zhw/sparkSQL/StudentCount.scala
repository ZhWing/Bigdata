package cn.zhw.sparkSQL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 使用Spark统计每个班级学生的人数,将统计好的结果保存到文件中
 *
 * 学生表字段描述：学号,姓名,年龄,性别,班级
 * 分数表字段描述：学号,科目名,分数
 * 科目表字段描述：科目名,总分
 *
 * 输出样式:
 * 班级,人数
 * 文科一班,39
 */
object StudentCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Student")

    val sc = new SparkContext(conf)

    val data: RDD[String] = sc.textFile("azy2/students.txt")
    // 统计每个班级学生的人数,将统计好的结果保存到文件中
    data
      .map(line => (line.split(",")(4), 1)) // k：班级 v：1
      .reduceByKey(_+_) // 根据班级分组计数
      .saveAsTextFile("azy2/out/StudentCount")
  }
}
