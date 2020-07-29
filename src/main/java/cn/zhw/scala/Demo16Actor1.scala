package cn.zhw.scala

import scala.actors.Actor

object Demo16Actor1 {
  def main(args: Array[String]): Unit = {
    println("main start")
    // 创建线程对象
    val worker = new Worker()
    worker.start()
    // 发送消息
    /**
     * 消息发送的过程是异步的
     * 1、不用等待对方接收成功
     * 2、不需要返回
     */
    worker ! "message"
    worker ! "message1"
    worker ! Login("张三", "123456")

    /**
     * 同步发送消息需要对方回复一个消息
     */
    val msgFun = worker !! 12
    val value = msgFun()
    println(s"接收回的消息$value")

    println("main end")
  }
}

class Worker extends Actor {
  override def act(): Unit = {
    println("Scala 多线程")

    while (true) {
      // 接收消息
      receive {
        case a: String => println(s"接收到的消息：$a")
        case a: Int => {
          println(s"接收到的消息：$a")
          // 回复消息
          sender ! "接收成功！"
        }
        case Login("张三", "123456") => println("登录成功！")
      }
    }
  }
}

// 样例类
case class Login(name: String, passwd: String)