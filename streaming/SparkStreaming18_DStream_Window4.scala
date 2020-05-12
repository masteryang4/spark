package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming18_DStream_Window4 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        ssc.checkpoint("scp")

        // 滑窗
        val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

        // 对窗口的数据进行计数，会使用checkpoint进行保存
        val countDS: DStream[Long] = socketDS.countByWindow(Seconds(6), Seconds(3))

        countDS.print()

        ssc.start()
        ssc.awaitTermination()

    }
}
