package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming12_DStream_Join {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        val socketDS1: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
        val socketDS2: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8888)

        val ds1: DStream[(String, Int)] = socketDS1.map((_,1))
        val ds2: DStream[(String, Int)] = socketDS2.map((_,1))

        val joinDS: DStream[(String, (Int, Int))] = ds1.join(ds2)

        joinDS.print()

        ssc.start()
        ssc.awaitTermination()

    }
}
