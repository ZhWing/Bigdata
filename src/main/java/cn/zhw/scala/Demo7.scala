package cn.zhw.scala

object Demo7 {
  def main(args: Array[String]): Unit = {
    val points = Points(1, 2)
    println(points)
    println(points.vx + " " +  points.vy)
    points.move(1, 2)
  }
}

/**
 * 样例类
 * 1、默认实现了序列化接口
 * 2、toString equals
 * 3、在编译的时候会加上伴生对象
 */
case class Points(vx: Int, vy: Int) {
  var x: Int = vx
  var y: Int = vy
  def move(mx: Int, my: Int): Unit = {
    x = x + mx
    y = y + my

    println("移动之后坐标(" + x + ", " + y + ")")
  }
}
