package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming19_DStream_Window5 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        ssc.checkpoint("scp")
        // 滑窗
        val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
        val wordToOneDS: DStream[(String, Int)] = socketDS.map(num=>("a", 1))
        val windowDS: DStream[(String, Int)] = wordToOneDS.reduceByKeyAndWindow(
            (x: Int, y: Int) => {
                val sum = x + y
                println( sum + "=" + x + "+" + y )
                sum
            },
            (x:Int, y:Int) => {
                val diff = x - y
                println( diff + "=" + x + "-" + y )
                diff
            },
            Seconds(6), Seconds(3)
        )
        windowDS.print()

        ssc.start()
        ssc.awaitTermination()

    }
}
