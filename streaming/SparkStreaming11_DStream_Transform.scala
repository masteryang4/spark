package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming11_DStream_Transform {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

        // TODO transform可以获取底层的RDD进行处理
        // TODO transform可以周期性的执行driver的代码逻辑

        // Code => Driver
//        val newDS: DStream[String] = socketDS.map(
//            dataString => {
//                // Code = Executor
//                "string : " + dataString
//            }
//        )

        // Code = Driver
        // JDBC.getData();
        val newDS1: DStream[String] = socketDS.transform(
            rdd => {
                // Code = Driver
                // JDBC.getData();
                println(Thread.currentThread().getName)
                rdd.map(
                    dataString => {
                        // Code = Executor
                        "string : " + dataString
                        // JDBC.updateData();
                    }
                )
            }
        )

        newDS1.print()

        ssc.start()
        ssc.awaitTermination()

    }
}
