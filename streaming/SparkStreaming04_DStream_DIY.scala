package com.atguigu.bigdata.spark.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming04_DStream_DIY {

    def main(args: Array[String]): Unit = {

        // TODO 配置对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")

        // TODO 环境对象
        val ssc = new StreamingContext(sparkConf, Seconds(5))

        // TODO 数据处理
        // 自定义数据采集器
        val myDS = ssc.receiverStream( new MyReceiver( "localhost", 9999 ) )
        val wordDS: DStream[String] = myDS.flatMap(_.split(" "))
        val wordToOneDS: DStream[(String, Int)] = wordDS.map( (_, 1) )
        val wordToCountDS: DStream[(String, Int)] = wordToOneDS.reduceByKey(_+_)
        wordToCountDS.print()

        // TODO 关闭连接环境
        ssc.start()
        ssc.awaitTermination()
    }
    /*
    自定义数据采集器
    模仿spark自带的socket采集器

    1. 继承Receiver ,设定泛型（采集数据的类型）, 传递参数
    2. 重写方法
     */
    // rdd cache, checkpoint
    class MyReceiver(host:String, port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
        private var socket: Socket = _

        // 接收数据
        def receive(): Unit = {

            val reader = new BufferedReader(
                new InputStreamReader(
                    socket.getInputStream,
                    "UTF-8"
                )
            )

            var s : String = null
            // 网络编程中，获取的数据没有null的概念
            // 如果网络编程中，需要明确告知服务器，客户端不再传数据，需要发送特殊的指令
            // 文件读取时，如果读到结束的时候，获取的结果为null
            while ( (s = reader.readLine()) != null ) {
                // 采集到数据后，进行封装(存储)
                if ( s != "-END-" ) {
                    store(s)
                } else {
                    // stop
                    // close
                    // 重启
                    //restart("")
                }

            }


        }
        // 启动采集器
        // 采集 & 封装
        override def onStart(): Unit = {

            socket = new Socket(host, port)

            new Thread("Socket Receiver") {
                setDaemon(true)
                override def run() { receive() }
            }.start()
        }

        override def onStop(): Unit = {

            if ( socket != null ) {
                socket.close()
            }
        }
    }
}
