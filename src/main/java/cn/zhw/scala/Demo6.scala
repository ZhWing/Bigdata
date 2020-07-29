package cn.zhw.scala

object Demo6 {
  def main(args: Array[String]): Unit = {
    val point = new Point(1, 1)
    point.move(10, 20)

    // 通过伴生对象创建类的对象
    val point1 = Point(1, 2)
    point1.move(1, 2)

    println("=" * 20)
    val location = new Location(1, 2, 3)
    location.move(1, 2, 3)
  }
}


// 默认参数自带 val 不可变
class Point(vx: Int,vy: Int) {
  var x: Int = vx
  var y: Int = vy
  def move(mx: Int, my: Int): Unit = {
    this.x = x + mx
    this.y = y + my

    println("移动之后坐标(" + x + ", " + y + ")")
  }
}

// 伴生对象
// 在创建对象的时候可以省略关键字 new
// 默认调用
object Point {
  def apply(vx: Int, vy: Int): Point = new Point(vx, vy)
}

class Location(xc: Int, yc: Int, zc: Int) extends Point(xc, yc) {
  var z = zc
    def move(mx: Int, my: Int, mz: Int): Unit = {
      this.x = x + mx
      this.y = y + my
      this.z = z + mz

      println("移动之后坐标(" + x + ", " + y + ", " + z + ")")
  }
}

object Location{
  def apply(xc: Int, yc: Int, zc: Int): Location = new Location(xc, yc, zc)
}
