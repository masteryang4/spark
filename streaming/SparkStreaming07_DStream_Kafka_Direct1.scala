package com.atguigu.bigdata.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming07_DStream_Kafka_Direct1 {

    def main(args: Array[String]): Unit = {

        // TODO 这种方式，可以保证数据不丢失，但是可能会出现数据重复消费

        // TODO 环境对象 - 从checkpoint中读取数据偏移量
        //                 checkpoint还保存了计算逻辑,不适合扩展功能
        //                 checkpoint会延续计算，但是可能会压垮内存
        //                 checkpoint一般的存储路径为HDFS，所以会导致小文件过多。性能受到影响
        // 不推荐使用
        val ssc: StreamingContext = StreamingContext.getActiveOrCreate("scp", () => getStreamingContext)
        // TODO 关闭连接环境
        ssc.start()
        ssc.awaitTermination()
    }
    def getStreamingContext () = {
        // TODO 配置对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")

        val ssc = new StreamingContext(sparkConf, Seconds(5))
        ssc.checkpoint("scp")

        // TODO 数据处理
        // 使用0.8版本的kafka - Direct方式 - 自动维护Offset
        // TODO 默认情况下，SparkStreaming采用checkpoint来保存kafka的数据偏移量
        // 访问kakfa会有相应的工具类
        val kafkaParamMap = Map(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "linux1:9092,linux2:9092,linux3:9092",
            ConsumerConfig.GROUP_ID_CONFIG -> "atguigu191125new"
        )
        val kafkaDS: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc,
            kafkaParamMap,
            Set("atguigu191125new")
        )
        kafkaDS.map(_._2).print()
        kafkaDS.print()

        ssc
    }
}
