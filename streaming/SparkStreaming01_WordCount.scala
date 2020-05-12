package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object SparkStreaming01_WordCount {

    def main(args: Array[String]): Unit = {

        // TODO 配置对象
        // SparkStreaming至少需要2个线程进行处理
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")

        // TODO 环境对象
        // Streaming SparkContext
        // Spark Streaming Context
        //val ssc = new StreamingContext(sparkConf, Duration(1000 * 5))
        // 创建对象的第二个参数表示数据的采集周期
        val ssc = new StreamingContext(sparkConf, Seconds(5))

        // TODO 数据处理
        // TODO 从数据源采集数据
        // 从Socket中获取的一行一行的数据
        val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

        // TODO 将采集数据进行WordCount的处理
        val wordDS: DStream[String] = socketDS.flatMap(_.split(" "))
        val wordToOneDS: DStream[(String, Int)] = wordDS.map( (_, 1) )
        val wordToCountDS: DStream[(String, Int)] = wordToOneDS.reduceByKey(_+_)

        // SparkStreaming默认的计算以采集周期中的数据为单位的，而不是采集到的所有数据
        wordToCountDS.print()

        // TODO 关闭连接环境
        // 关闭SparkStreaming是可以的，但是一般的程序升级或出现故障时
        // 所以一般情况下是不能关闭的，并且不能让driver程序结束,需要让driver程序等待
        //ssc.stop()

        // 启动数据采集器，开始采集数据
        // 启动数据处理环境
        ssc.start()

        // 让driver程序等待数据处理的停止或异常时，才会继续执行
        ssc.awaitTermination()

    }
}
