package com.atguigu.bigdata.spark.streaming

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming09_DStream_Kafka_Direc3 {

    def main(args: Array[String]): Unit = {

        // TODO 配置对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
        val ssc = new StreamingContext(sparkConf, Seconds(5))

        // TODO 数据处理
        // SparkStreaming消费Kafka数据时，手动维护offset的思路

        // TODO 1. 从指定的位置获取当前业务中保存的数据偏移量
        // mysql => message offset => 5
        // TODO 2. 从kafka中对应的分区里根据偏移量获取数据
        // topicAndPartition => topic : xxx, partition : 0, offset : 5

        // TODO 3. 消费数据时，需要将消费数据的偏移量拿到。
        // KafkaRDD => offsetRange => (5, 100)

        // TODO 4. 执行业务操作。要求，偏移量的更新和业务要求在同一个事务中
        // Tx start
        //    service
        //    commit - offset -> mysql
        // Tx commit
        // TODO 4.1 如果不使用事务，那么可能业务成功，但是offset提交失败
        //          会导致数据重复消费
        // TODO 4.2 如果不使用事务，那么可能offset提交成功，但是业务失败
        //          会导致数据丢失
        // TODO 4.3 分布式事务， 如果中间出现shuffle，怎么办？
        //          所以需要将数据拉取到driver端进行事务操作，保证数据不会出现问题。
        //          这样会导致driver的性能下降，所以其实不是一个好的选择。
        // SparkStreaming => 基本要求： 不丢失数据
        // Flink => 数据精准一次性处理。

        ssc.start()
        ssc.awaitTermination()
    }
}
