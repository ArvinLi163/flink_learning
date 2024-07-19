package cn.lgy.chapter05

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

/**
 * @description:
 * @author: ArvinLi
 * */
object TransUDF {
  def main(args: Array[String]): Unit = {
    //执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.fromElements(Event("Mary", "./home", 1000L),
      Event("Tom", "./cart", 2000L),
      Event("Tom", "./cart", 3000L),
      Event("Alice", "./cart", 3000L),
      Event("Mary", "./prod?id=1", 4000L),
      Event("Mary", "./prod?id=2", 6000L),
      Event("Mary", "./prod?id=2", 7000L)
    )
    //方式一
//    stream.filter(new MyFilterFunction)
//      .print("1")
    //传参
    stream.filter(new MyFilterFunction("prod"))
      .print("1")

    //方式二 匿名类
    stream.filter(new FilterFunction[Event] {
      override def filter(t: Event) = t.url.contains("prod")
    }).print("2")

    env.execute()
  }
  class MyFilterFunction(key:String) extends FilterFunction[Event]{
    override def filter(t: Event): Boolean = t.url.contains(key)
  }
}
