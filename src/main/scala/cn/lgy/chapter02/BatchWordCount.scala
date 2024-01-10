package cn.lgy.chapter02

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment, GroupedDataSet, createTypeInformation}

/**
 * @description: DataSet API在后面的版本中使用的越来越少，用的比较多的是DataStream API
 * @author: bansheng
 * @date: 2024/01/10 16:05
 * */
object BatchWordCount {
  def main(args: Array[String]): Unit = {
    //1.创建一个执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //2.读取文件
    val lineDataSet: DataSet[String] = env.readTextFile("input/words.txt", "UTF-8")
    //3.转换
    val wordToOne: DataSet[(String, Int)] = lineDataSet.flatMap(_.split(" ")).map((_, 1))
    //4.分组 0代表元组中的第一个元素，即按照Key进行分组
    val wordGroup: GroupedDataSet[(String, Int)] = wordToOne.groupBy(0)
    //5.聚合 1代表的是索引位置
    val result: AggregateDataSet[(String, Int)] = wordGroup.sum(1)
    //6.打印输出
    result.print()
  }
}
