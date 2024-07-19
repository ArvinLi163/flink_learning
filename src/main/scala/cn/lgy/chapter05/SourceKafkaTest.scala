package cn.lgy.chapter05

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

/**
 * @description:
 * @author: ArvinLi
 * */
object SourceKafkaTest {
  def main(args: Array[String]): Unit = {

    //创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //kafka相关配置
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","hadoop102:9092")
    properties.setProperty("group.id","consumer-group")
    //读取数据源
    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("clicks", new SimpleStringSchema(), properties))

    //打印输出
    stream.print()

    //执行
    env.execute()
  }
}
