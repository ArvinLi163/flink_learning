package cn.lgy.chapter05

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @description:
 * @author: ArvinLi
 * */
object SinkToRedis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //读取数据
    val stream: DataStream[Event] = env.addSource(new ClickSource)
   //创建一个FlinkJedisPoolConfig
   val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("localhost").build()

    //写入redis
    stream.addSink(new RedisSink[Event](conf,new MyRedisMapper))

    env.execute()
  }
  //实现RedisMapper接口
  class MyRedisMapper extends RedisMapper[Event]{
    override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET,"clicks")

    override def getKeyFromData(t: Event): String = t.user

    override def getValueFromData(t: Event): String = t.url
  }
}
