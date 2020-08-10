package cn.zhw.spark.test

import org.apache.spark.{SparkConf, SparkContext}
object MovieData4_1 {
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

    val data1 = data.map(f=> {
      if(f(1).indexOf("2019-昨天")==(-1)){
        (f(1),1)
      }else{
        f(1)="2019-昨天"
        ("2019-昨天",1)
      }
    })
    //      根据日期做reduce，然后排序
    data1.reduceByKey(_+_).collect.sortBy(f=>f._1).foreach(println)
    // 写入数据到hdfs系统
    //      data1.saveAsTextFile("hdfs://192.168.220.128:9000/test/result2")
  }
}
