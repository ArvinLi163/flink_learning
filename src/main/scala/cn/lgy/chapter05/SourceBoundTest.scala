package cn.lgy.chapter05

import org.apache.flink.streaming.api.scala._

/**
 * @description: 有界source
 * @author: ArvinLi
 * */
object SourceBoundTest {
  def main(args: Array[String]): Unit = {
    //1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //2.从元素中读取数据
    val stream: DataStream[Int] = env.fromElements(1, 2, 3, 4)

    val stream1: DataStream[Event] = env.fromElements(Event("Mary", "./home", 1000L),
      Event("Tom", "./cart", 2000L))
    val clicks=List(Event("Mary", "./home", 1000L), Event("Tom", "./cart", 2000L))
    val stream2: DataStream[Event] = env.fromCollection(clicks)
     //从文件中读取数据
     val stream3: DataStream[String] = env.readTextFile("input/click.txt")

    //打印输出
    stream.print("number")
    stream1.print("1")
    stream2.print("2")
    stream3.print("3")

    //执行
    env.execute()
  }
}

//样例类
case class Event(user: String, url: String, timestamp: Long)
