package cn.zhw.spark.test

import org.apache.spark.{SparkConf, SparkContext}

object MovieData4_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.set("spark.master", "local")
    conf.set("spark.app.name", "spark demo")
    val sc = new SparkContext(conf);
    // 读取hdfs数据
    val textFileRdd = sc.textFile("hdfs://192.168.247.137:9000/data/log_movie.txt")
    val newFileRdd = textFileRdd.filter(f=>f.indexOf("上海堡垒")!=(-1))
    //    将字符串分割
    val data = newFileRdd.map(f=>f.split(","))

    //    取出性别这一列
    val data1 = data.map(
      // 取出转发数，博主粉丝数对应的字段数据
      f=>(f(4).toLong,f(5),f(7))
    )
    //      根据日期做reduce，然后排序
    val data2 = data1.sortBy(_._1,false)
    data2.take(5).foreach(println)
  }
}
