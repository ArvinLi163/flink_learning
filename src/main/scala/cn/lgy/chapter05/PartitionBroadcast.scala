package cn.lgy.chapter05

import org.apache.flink.streaming.api.scala._

/**
 * @description:
 * @author: ArvinLi
 * */
object PartitionBroadcast {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //读取数据
    val stream: DataStream[Event] = env.addSource(new ClickSource)
    //
    stream.broadcast.print("broadcast").setParallelism(4)
    stream.global.print("global").setParallelism(4)

    env.execute()
  }
}
