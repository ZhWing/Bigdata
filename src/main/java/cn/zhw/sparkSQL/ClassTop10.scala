package cn.zhw.sparkSQL

import org.apache.spark.sql.SparkSession

/**
 * 使用 Spark sql 统计每个班级总分排名前十的学生，将统计好的结果保存到文件中
 *
 * 输出样式：
 *    班级,姓名,总分
 *    文科一班,张三,400
 */
object ClassTop10 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("ClassTop10")
      .getOrCreate()

    // 学生表字段描述：学号, 姓名, 年龄, 性别, 班级
    spark.read
      .option("sep", ",")
      .schema("id string, name string, age int, sex string, class string")
      .csv("azy2/students.txt")
      .createOrReplaceTempView("student")

    // 分数表字段描述：学号, 科目名, 分数
    spark.read
      .option("sep", ",")
      .schema("id string, sname string, score int")
      .csv("azy2/score.txt")
      .createOrReplaceTempView("score")

    // 班级, 姓名, 总分
    val result = spark.sql(
      """
        |select class, name, sum_score from
        |(select class, name, sum_score, row_number() over(partition by class order by sum_score desc) as rank from
        |(select id, sum(score) sum_score from score group by id) a join student b on a.id=b.id) c
        |where rank <= 10
        |""".stripMargin)

    result.show
    result.write.csv("azy2/out/top10")

    spark.stop()
  }
}
