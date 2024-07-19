package cn.lgy.chapter05

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._

/**
 * @description:
 * @author: ArvinLi
 * */
object PartitionCustom {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //读取数据
    val stream: DataStream[Int] = env.fromElements(1,2,3,4,5,6,7,8)
    //自定义重分区策略
    stream.partitionCustom(new Partitioner[Int] {
      override def partition(k: Int, i: Int) = {
        k % 2
      }
    },data => data).print().setParallelism(4)

    env.execute()
  }
}
