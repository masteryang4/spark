package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object SparkStreaming03_DStream_File {

    def main(args: Array[String]): Unit = {

        // TODO 配置对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")

        // TODO 环境对象
        val ssc = new StreamingContext(sparkConf, Seconds(5))

        // TODO 数据处理
        // 从文件夹中读取新的文件数据，功能不稳定 ，所以不推荐使用
        // flume更加专业，所以生产环境，监控文件或目录的变化，采集数据都使用flume
        val fileDS: DStream[String] = ssc.textFileStream("in")
        val wordDS: DStream[String] = fileDS.flatMap(_.split(" "))
        val wordToOneDS: DStream[(String, Int)] = wordDS.map( (_, 1) )
        val wordToCountDS: DStream[(String, Int)] = wordToOneDS.reduceByKey(_+_)
        wordToCountDS.print()

        // TODO 关闭连接环境
        ssc.start()
        ssc.awaitTermination()
    }
}
