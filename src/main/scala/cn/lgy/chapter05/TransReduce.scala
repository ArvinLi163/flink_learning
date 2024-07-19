package cn.lgy.chapter05

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._

/**
 * @description:
 * @author: ArvinLi
 * */
object TransReduce {
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
    //reduce归约聚合，提取当前最活跃的用户
    stream.map(data => (data.user, 1L))
      .keyBy(_._1) //按照用户进行分组
      .reduce(new MySum)
      .keyBy(data => true) //所用用户分到同一个组中
      .reduce((state, data) => if (data._2 >= state._2) data else state) //选取当前最活跃的用户
      .print()

    env.execute()
  }

  class MySum extends ReduceFunction[(String, Long)] {
    override def reduce(t: (String, Long), t1: (String, Long)): (String, Long) = (t._1, t._2 + t1._2)
  }
}
