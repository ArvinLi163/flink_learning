package cn.lgy.chapter05

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

/**
 * @description:
 * @author: ArvinLi
 * */
object TransFilter {
  def main(args: Array[String]): Unit = {
    //执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[Event] = env.fromElements(Event("Mary", "./home", 1000L),
      Event("Tom", "./cart", 2000L))

    //过滤用户为Mary的点击事件
    stream.filter(_.user=="Mary").print("1")
    stream.filter(new UserFilter).print("2")

    env.execute()
  }

  //自定义FilterFunction接口
  class UserFilter extends FilterFunction[Event]{
    override def filter(t: Event): Boolean = t.user=="Tom"
  }
}

