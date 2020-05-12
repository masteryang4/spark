package com.atguigu.bigdata.spark.streaming

import java.sql.{DriverManager, PreparedStatement, ResultSet}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object SparkStreaming23_Stop {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
        // TODO 配置优雅地关闭
        sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")

        val ssc = new StreamingContext(sparkConf, Seconds(5))

        val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

        val wordDS: DStream[String] = socketDS.flatMap(_.split(" "))
        val wordToOneDS: DStream[(String, Int)] = wordDS.map( (_, 1) )
        val wordToCountDS: DStream[(String, Int)] = wordToOneDS.reduceByKey(_+_)

        wordToCountDS.print()

        ssc.start()

        new Thread(
            new Runnable {
                override def run(): Unit = {
                    // TODO SparkStreaming是可以停止。但是停止的逻辑代码的位置？
                    // TODO stop方法不能放置在driver的主线程中。
                    // TODO 直接调用ssc的stop方法是不可以的。需要循环判断sparkStreaming是否应该关闭
                    while ( true ) {
                        // TODO 在Driver端应该设置标记，让当前关闭线程可以访问。可以动态改变状态。
                        // TODO 但是Driver端的标记何时更新，由谁更新都是不确定的。
                        // TODO 所以一般标记不是放置在Driver端，而是在第三方软件中：redis,zk,mysql,hdfs

                        Class.forName("com.mysql.jdbc.Driver")
                        // TODO 建立链接和操作对象
                        // TODO 所有的连接对象都不支持序列化操作
                        val conn =
                        DriverManager.getConnection(
                            "jdbc:mysql://linux1:3306/rdd",
                            "root","000000")
                        val sql = "select age from user where id = 1"
                        val statement: PreparedStatement = conn.prepareStatement(sql)
                        val rs: ResultSet = statement.executeQuery()
                        rs.next()
                        val age: Int = rs.getInt(1)
                        if ( age <= 20 ) {

                            // TODO 判断SSC的状态
                            val state: StreamingContextState = ssc.getState()
                            if ( state == StreamingContextState.ACTIVE ) {
                                println("SparkStreaming的环境准备关闭...")
                                // TODO 优雅地关闭SSC
                                // 将现有的数据处理完再关闭就是优雅地关闭
                                ssc.stop(true, true)
                                System.exit(0)
                            }
                        }

                        Thread.sleep(1000 * 5)
                    }

                }
            }
        ).start()

        ssc.awaitTermination()


        // TODO Thread 线程停止的方式？run方法执行完毕
        // 为什么不调用stop方法停止线程？因为会出现数据安全问题
        // i++ => 1), 2)
        // new Thread().stop()



    }
}
