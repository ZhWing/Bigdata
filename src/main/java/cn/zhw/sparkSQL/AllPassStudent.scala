package cn.zhw.sparkSQL

import org.apache.spark.sql.SparkSession

/**
 * 统计每科都及格的学生
 *
 * 输出样式:
 *    学号, 姓名, 班级, 科目名, 分数
 *    1500100001, 施笑槐, 文科六班, 语文, 80
 */
object AllPassStudent {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("AllPassStudent")
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

    // 科目表字段描述：科目名,总分
    spark.read
      .option("sep", ",")
      .schema("sname string, num int")
      .csv("azy2/course.txt")
      .createOrReplaceTempView("course")

    val result = spark.sql(
      """
        |select a.id, name, class, sname, score from student a join score b on a.id=b.id
        |where a.id in
        |(select id from
        |(select id, count(*) pass from score c join course d on c.sname=d.sname where score > num*0.6
        |group by id having pass=6) e)
        |""".stripMargin)

    result.show
    result.write.csv("azy2/out/AllPassStudent")
  }
}
