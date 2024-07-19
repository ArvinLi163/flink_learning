package cn.lgy.chapter05

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

/**
 * @description:
 * @author: ArvinLi
 * */
object TransMap {
  def main(args: Array[String]): Unit = {
   //执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[Event] = env.fromElements(Event("Mary", "./home", 1000L),
      Event("Tom", "./cart", 2000L))
    //转换  每一次点击事件对应一个用户名
    //方法一
    stream.map(_.user).print("1")

    //方法二
    stream.map(new UserExtractor).print("2")

    //执行
    env.execute()
  }

  //实现MapFunction接口
  class UserExtractor extends MapFunction[Event, String] {
    override def map(t: Event): String = t.user
  }
}

