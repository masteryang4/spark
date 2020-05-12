package com.atguigu.bigdata.spark.streaming

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming08_DStream_Kafka_Direc2 {

    def main(args: Array[String]): Unit = {

        // TODO 配置对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
        val ssc = new StreamingContext(sparkConf, Seconds(5))

        // TODO 数据处理
        // 使用0.8版本的kafka - Direct方式 - 手动维护Offset
        // 所谓的手动维护，其实就是开发人员自己获取偏移量，并进行保存处理。
        // 通过保存的偏移量，可以动态获取kafka中指定位置的数据
        // offset会保存到kakfa集群的系统主题中__consumer_offsets
        val kafkaMap = Map(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "linux1:9092,linux2:9092,linux3:9092",
            ConsumerConfig.GROUP_ID_CONFIG -> "atguigu191125123"
        )
        val fromOffsets = Map(
            (TopicAndPartition("atguigu191125new", 0), 0L),
            (TopicAndPartition("atguigu191125new", 1), 1L),
            (TopicAndPartition("atguigu191125new", 2), 2L)
        )
        // TODO 从kafka中获取指定topic中指定offset的数据
        val kafkaDS: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
            ssc,
            kafkaMap,
            fromOffsets,
            (m:MessageAndMetadata[String, String]) => m.message()
        )

        var offsetRanges = Array.empty[OffsetRange]

        // 转换
        // 获取偏移量，一定要在最初的逻辑中获取，防止数据处理完毕后，无偏移量信息
        kafkaDS.transform(rdd => {
            // 获取RDD中的偏移量范围
            // 默认Spark中的RDD是没有offsetRanges方法，所以必须转换类型后才能使用
            // RDD 和 HasOffsetRanges有关系
            offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            rdd
        }).foreachRDD(rdd=>{
            for (o <- offsetRanges) {
                println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
            }
            rdd.foreach(println)
        })

        ssc.start()
        ssc.awaitTermination()
    }
}
