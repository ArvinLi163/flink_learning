package cn.lgy.chapter05

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala._

/**
 * @description:
 * @author: ArvinLi
 * */
object TransAggr {
  def main(args: Array[String]): Unit = {
    //执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    val stream: DataStream[Event] = env.fromElements(Event("Mary", "./home", 1000L),
      Event("Tom", "./cart", 2000L),
      Event("Alice", "./cart", 3000L),
      Event("Mary", "./prod?id=1", 4000L),
      Event("Mary", "./prod?id=2", 6000L),
      Event("Mary", "./prod?id=2", 6000L)
    )

//    stream.keyBy(_.user).print("1")
    stream.keyBy(new MyKeySelector).maxBy("timestamp")
      .print()

    env.execute()
  }

  class MyKeySelector extends KeySelector[Event,String]{
    override def getKey(in: Event): String = {
      in.user
    }
  }
}
