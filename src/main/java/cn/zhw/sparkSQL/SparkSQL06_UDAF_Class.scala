package cn.zhw.sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.network.protocol.Encoders
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession, TypedColumn}

object SparkSQL05_UDAF {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 创建聚合函数对象
    val udfa = new MyAvgClassFunction
    // 将聚合函数转换为查询的列
    val avgCol: TypedColumn[UserBean, Double] = udfa.toColumn.name("age")

    val frame: DataFrame = spark.read.json("data/test.json")
    val userDS: Dataset[UserBean] = frame.as[UserBean]
    // 应用函数
    userDS.select(avgCol).show


    spark.stop()
  }
}
case class UserBean(name: String, age: BigInt)
case class AvgBuffer(var sum: Int, var count: Int)

// 声明用户自定义聚合函数(强类型)
// 1、继承 Aggregator, 设定泛型
// 2、实现方法
class MyAvgClassFunction extends Aggregator[UserBean, AvgBuffer, Double] {
  // 初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0)
  }

  // 集合数据
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
    b.sum += a.age
    b.count += 1

    b
  }

  // 缓冲区的合并操作
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum += b2.sum
    b1.count += b2.count

    b1
  }

  // 完成计算
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}