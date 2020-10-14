package cn.zhw.sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL02_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val frame: DataFrame = spark.read.json("data/test.json")

    // 创建 RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 20), (2, "lisi", 21), (3, "wangwu", 22)))
    import spark.implicits._

    // 转换为 DF
    val df: DataFrame = rdd.toDF("id", "name", "age")

    // 转换为 DS
    val ds: Dataset[User] = df.as[User]

    // 转换为 DF
    val df1: DataFrame = ds.toDF()

    // 转换为 RDD
    val rdd1: RDD[Row] = df1.rdd

    rdd1.foreach(row => {
      // 获取数据时，可以通过索引访问数据
      println(row.getString(1))
    })

    spark.stop()
  }
}
case class User(id: Int, name: String, age: Int)