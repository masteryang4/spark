package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming17_DStream_Window3 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        // 滑窗
        val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
        val wordToOneDS: DStream[(String, Int)] = socketDS
                .flatMap(_.split(" "))
                .map((_, 1))
        val windowDS: DStream[(String, Int)] = wordToOneDS.reduceByKeyAndWindow(
            (x: Int, y: Int) => x + y, Seconds(6), Seconds(3)
        )
        windowDS.print()

        ssc.start()
        ssc.awaitTermination()

    }
}
