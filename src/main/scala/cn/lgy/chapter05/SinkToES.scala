package cn.lgy.chapter05

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

import java.util

/**
 * @description:
 * @author: ArvinLi
 * */
object SinkToES {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //读取数据
    val stream: DataStream[Event] = env.fromElements(Event("Mary", "./home", 1000L),
      Event("Tom", "./cart", 2000L),
      Event("Tom", "./cart", 3000L),
      Event("Alice", "./cart", 3000L),
      Event("Mary", "./prod?id=1", 4000L),
      Event("Mary", "./prod?id=2", 6000L),
      Event("Mary", "./prod?id=2", 7000L)
    )

    //es集群列表
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost",9200))

    //定义ElasticsearchSinkFunction
    val esfun = new ElasticsearchSinkFunction[Event]{
      override def process(t: Event, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer) = {
        val data = new util.HashMap[String,String]()
        data.put(t.user,t.url)
        //包装http请求
        val request: IndexRequest = Requests.indexRequest()
          .index("clicks") //es索引名
          .source(data)
//          .`type`("event")

        //发起请求
        requestIndexer.add(request)
      }
    }


    //写入数据
    stream.addSink(new ElasticsearchSink.Builder[Event](httpHosts,esfun).build())
    env.execute()
  }
}
