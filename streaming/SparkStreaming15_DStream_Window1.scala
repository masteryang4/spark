package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming15_DStream_Window1 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        // 滑窗
        val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

        // 设定窗口。将2个采集周期的数据当成一个整体进行处理
        // 默认窗口是可以滑动的。滑动的幅度为一个采集周期
        // 可以动态改变滑动幅度
        // 如果两个窗口移动过程中，没有重合的数据，称之为滚动窗口
        // window方法的第一个参数表示窗口的范围大小，以采集周期为单位
        // window方法的第二个参数表示窗口的滑动幅度，也表示计算的周期
        val windowDS: DStream[String] = socketDS.window(
            Seconds(6), Seconds(3))
        windowDS
            .flatMap(_.split(" "))
            .map((_,1))
            .reduceByKey(_+_)
            .print()

        ssc.start()
        ssc.awaitTermination()

    }
}
