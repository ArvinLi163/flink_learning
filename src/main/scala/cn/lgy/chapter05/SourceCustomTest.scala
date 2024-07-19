package cn.lgy.chapter05

import org.apache.flink.streaming.api.scala._

/**
 * @description:
 * @author: ArvinLi
 * */
object SourceCustomTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[Event] = env.addSource(new ClickSource)

    stream.print()

    env.execute()
  }
}
