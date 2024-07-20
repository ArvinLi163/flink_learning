package cn.lgy.chapter05

import com.mysql.jdbc.JDBC4PreparedStatement
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.scala._

import java.sql.PreparedStatement

/**
 * @description:
 * @author: ArvinLi
 * */
object SinkToMysql {
  def main(args: Array[String]): Unit = {
    //执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.fromElements(Event("Mary", "./home", 1000L),
      Event("Tom", "./cart", 2000L),
      Event("Tom", "./cart", 3000L),
      Event("Alice", "./cart", 3000L),
      Event("Mary", "./prod?id=1", 4000L),
      Event("Mary", "./prod?id=2", 6000L),
      Event("Mary", "./prod?id=2", 7000L)
    )

    stream.addSink(JdbcSink.sink("insert into clicks(user,url) values(?,?)",
      new JdbcStatementBuilder[Event] {
        override def accept(t: PreparedStatement, u: Event) = {
          t.setString(1,u.user)
          t.setString(2,u.url)
        }
      },
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl("jdbc:mysql://localhost:3306/test")
        .withDriverName("com.mysql.jdbc.Driver")
        .withUsername("root")
        .withPassword("123456")
        .build()
    ))

    env.execute()
  }
}
