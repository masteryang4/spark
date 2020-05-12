package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object SparkStreaming02_DStream_Queue {

    def main(args: Array[String]): Unit = {

        // TODO 配置对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")

        // TODO 环境对象
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        // TODO 数据处理
        val que = new mutable.Queue[RDD[String]]()
        val queDS: InputDStream[String] = ssc.queueStream(que)
        queDS.print()

        // TODO 关闭连接环境
        ssc.start()

        println("queue append item")
        for ( i <- 1 to 5 ) {
            val rdd = ssc.sparkContext.makeRDD(List("1","2"))
            que += rdd
            Thread.sleep(2000)
        }

        // block
        ssc.awaitTermination()
    }
}
