package cn.lgy.chapter05

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

/**
 * @description:
 * @author: ArvinLi
 * */
object PartitionRescale {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //读取数据
    val stream: DataStream[Int] = env.addSource(new RichParallelSourceFunction[Int] {
      override def run(sourceContext: SourceFunction.SourceContext[Int]) = {
        for (i <- 0 to 7) {
          //根据运行时环境知道数据由那些并行子任务执行
          if (getRuntimeContext.getIndexOfThisSubtask == (i + 1) % 2) {
            //往下游发送数据
            sourceContext.collect(i + 1)
          }
        }
      }

      override def cancel() = ???
    }).setParallelism(2)

    stream.rescale.print("rescale").setParallelism(4)

    env.execute()
  }
}
