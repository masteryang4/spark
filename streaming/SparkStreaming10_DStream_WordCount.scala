package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming10_DStream_WordCount {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

        // TODO 可以将DStream转换为RDD进行操作。
        // DStream => old RDD => new RDD => new DStream
        val resultDS: DStream[(String, Int)] = socketDS.transform(
            rdd => {
                val flatRDD = rdd.flatMap(_.split(" "))
                val mapRDD = flatRDD.map((_, 1))
                val reduceRDD = mapRDD.reduceByKey(_ + _)
                reduceRDD
            }
        )
        resultDS.print()

        ssc.start()
        ssc.awaitTermination()

    }
}
