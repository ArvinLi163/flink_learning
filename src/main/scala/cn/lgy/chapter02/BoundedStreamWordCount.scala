package cn.lgy.chapter02

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, GroupedDataSet, createTypeInformation}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

/**
 * @description: 有界流处理
 * @author: bansheng
 * @date: 2024/01/10 16:05
 * */
object BoundedStreamWordCount {
  def main(args: Array[String]): Unit = {
    //1.创建一个流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2.读取文件
    val lineDataStream: DataStream[String] = env.readTextFile("input/words.txt", "UTF-8")
    //3.转换
    val wordToOne: DataStream[(String, Int)] = lineDataStream.flatMap(_.split(" ")).map((_, 1))
    //4.按照Key进行分组
    val wordGroup: KeyedStream[(String, Int), String] = wordToOne.keyBy(data => data._1)
    //5.聚合
    val result: DataStream[(String, Int)] = wordGroup.sum(1)
    //6.打印输出
    result.print()

    //执行任务
    env.execute()
  }
}
