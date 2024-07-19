package cn.lgy.chapter05

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

import java.util.Calendar
import scala.util.Random

/**
 * @description:ParallelSourceFunction并行
 * @author: ArvinLi
 * */
class ClickSource extends SourceFunction[Event]{
  //标志位
  var running = true
  override def run(sourceContext: SourceFunction.SourceContext[Event]): Unit = {
   //随机生成器
   val random = new Random()
   //定义数据随机选择的范围
   val users: Array[String] =Array("Mary","Alice","Bob","Cary")
    val urls: Array[String] = Array("./home","./cart","./fav","./prod","./prod?id=1")
    while (running){
      val event = Event(users(random.nextInt(users.length)),urls(random.nextInt(urls.length)),Calendar.getInstance().getTimeInMillis)
      //调用sourceContext的方法向下游发送数据
      sourceContext.collect(event)

      //每个1秒发送一条数据
      Thread.sleep(1000)
    }

  }

  override def cancel(): Unit = running = false
}
