package cn.lgy.chapter05

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

import java.util.Properties

/**
 * @description:
 * @author: ArvinLi
 * */
object SinkToKafka {
  def main(args: Array[String]): Unit = {
    //执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //读取数据
    //    val stream: DataStream[String] = env.readTextFile("input/click.txt")
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id","consumer-group")
    val stream: DataStream[String] = env.addSource(
      new FlinkKafkaConsumer[String]("clicks", new SimpleStringSchema(), properties)
    ).map(data=>{
      val fields= data.split(",")
      Event(fields(0).trim,fields(1).trim,fields(2).toLong).toString
    })
    //往kafka写入数据
    stream.addSink(
      new FlinkKafkaProducer[String]("hadoop102:9092", "events", new SimpleStringSchema())
    )
    env.execute()
  }
}
