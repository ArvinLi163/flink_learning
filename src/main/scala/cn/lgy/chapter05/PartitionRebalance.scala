package cn.lgy.chapter05

import org.apache.flink.streaming.api.scala._

/**
 * @description:
 * @author: ArvinLi
 * */
object PartitionRebalance {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //读取数据
    val stream: DataStream[Event] = env.addSource(new ClickSource)
    //轮询之后打印
//    stream.rebalance.print("rebalance").setParallelism(4)
    stream.print("rebalance").setParallelism(4)

    env.execute()
  }
}
