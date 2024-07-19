package cn.lgy.chapter05

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment._
import org.apache.flink.util.Collector

/**
 * @description:
 * @author: ArvinLi
 * */
object TransFlatMap {
  def main(args: Array[String]): Unit = {
    //执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[Event] = env.fromElements(Event("Mary", "./home", 1000L),
      Event("Tom", "./cart", 2000L),
      Event("Alice", "./cart", 3000L)
    )

    stream.flatMap(new User).print("1")
    env.execute()
  }

  class User extends FlatMapFunction[Event,String]{
    override def flatMap(t: Event, collector: Collector[String]): Unit = {
      if(t.user=="Mary"){
        collector.collect(t.user)
      }else if(t.user=="Tom"){
        collector.collect(t.user)
        collector.collect(t.url)
      }
    }
  }
}
