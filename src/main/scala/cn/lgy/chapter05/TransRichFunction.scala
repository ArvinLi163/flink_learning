package cn.lgy.chapter05

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
 * @description:
 * @author: ArvinLi
 * */
object TransRichFunction {
  def main(args: Array[String]): Unit = {
    //执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val stream: DataStream[Event] = env.fromElements(Event("Mary", "./home", 1000L),
      Event("Tom", "./cart", 2000L),
      Event("Tom", "./cart", 3000L),
      Event("Alice", "./cart", 3000L),
      Event("Mary", "./prod?id=1", 4000L),
      Event("Mary", "./prod?id=2", 6000L),
      Event("Mary", "./prod?id=2", 7000L)
    )

    stream.map(new MyMap).print()

    env.execute()
  }

  class MyMap extends RichMapFunction[Event, Long] {
    override def map(in: Event): Long = in.timestamp

    override def open(parameters: Configuration): Unit = {
      println("序列号是:" + getRuntimeContext.getIndexOfThisSubtask + "的任务开始")
    }

    override def close(): Unit = {
      println("序列号是:" + getRuntimeContext.getIndexOfThisSubtask + "的任务结束")
    }
  }
}
