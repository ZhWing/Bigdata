package cn.zhw.scala

import sun.plugin2.message.Message

import scala.actors.Actor
/**
 * 线程之间的通信
 */
object Demo16Actor2 {
  def main(args: Array[String]): Unit = {
    val worker = new Worker0()
    val master = new Master(worker)
    worker.start()
    master.start()
  }
}

case class Message(msg: String, actor: Actor)

class Worker0 extends Actor {
  override def act(): Unit = {
    receive {
      case Message(msg, actor) => {
        println(s"Worker 接收道德消息：$msg")

        // 给 Master 回复一个消息
        actor ! "Yes!"
      }
    }
  }
}

class Master(worker: Actor) extends Actor {
  override def act(): Unit = {
    val msgFun = worker ! Message("心跳", this)

    // 接收 Worker 返回来的消息
    receive {
      case s: String => println(s"Message 接收到的消息:$s")
    }
  }
}