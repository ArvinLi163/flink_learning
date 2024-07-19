package cn.lgy.chapter05

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala._

import java.util.concurrent.TimeUnit

/**
 * @description:
 * @author: ArvinLi
 * */
object SinkToFile {
  def main(args: Array[String]): Unit = {
    //执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    val stream: DataStream[Event] = env.fromElements(Event("Mary", "./home", 1000L),
      Event("Tom", "./cart", 2000L),
      Event("Tom", "./cart", 3000L),
      Event("Alice", "./cart", 3000L),
      Event("Mary", "./prod?id=1", 4000L),
      Event("Mary", "./prod?id=2", 6000L),
      Event("Mary", "./prod?id=2", 7000L)
    )
    //StreamingFileSink
    val sink: StreamingFileSink[String] = StreamingFileSink
      .forRowFormat(new Path("./output"), new SimpleStringEncoder[String]("utf-8"))
      //指定滚动策略
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(TimeUnit.MINUTES.toMinutes(15))
          .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
          .withMaxPartSize(1024 * 1024 * 1024)
          .build()
      )
      .build()
    stream.map(_.toString).addSink(sink)

    env.execute()
  }
}
