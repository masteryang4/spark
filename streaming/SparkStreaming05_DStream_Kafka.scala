package com.atguigu.bigdata.spark.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming05_DStream_Kafka {

    def main(args: Array[String]): Unit = {

        // TODO 配置对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")

        // TODO 环境对象
        val ssc = new StreamingContext(sparkConf, Seconds(5))

        // TODO 数据处理
        // 使用0.8版本的kafka - 接收器方式
        // 访问kakfa会有相应的工具类
        val kafkaDS: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
            ssc,
            "linux1:2181,linux2:2181,linux3:2181",
            "atguigu191125",
            Map("atguigu191125" -> 3)
        )

        // Kafka消息传递的时候以k-v对
        // k - 传值的时候提供的，默认为null,主要用于分区
        // v - message
        kafkaDS.map(_._2).print()

        // TODO 关闭连接环境
        ssc.start()
        ssc.awaitTermination()
    }
}
