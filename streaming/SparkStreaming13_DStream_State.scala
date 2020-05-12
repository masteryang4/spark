package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming13_DStream_State {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        ssc.checkpoint("scp")
        val socketDS = ssc.socketTextStream("localhost", 9999)

        val wordDS: DStream[String] = socketDS.flatMap(_.split(" "))

        val wordToOneDS: DStream[(String, Int)] = wordDS.map((_,1))
        // TODO 使用有状态操作updateStateByKey保存数据
        // SparkStreaming的状态保存依赖的是checkpoint,所以需要设定相关路径
        val wordToCountDS: DStream[(String, Long)] = wordToOneDS.updateStateByKey[Long](
            // 累加器 = 6
            // UDAF = 8
            // TODO 第一个参数表示相同key的value数据集合
            // TODO 第二个参数表示相同key的缓冲区的数据
            (seq: Seq[Int], buffer: Option[Long]) => {
                // TODO 返回值表示更新后的缓冲区的值
                val newBufferValue = buffer.getOrElse(0L) + seq.sum
                Option(newBufferValue)
            }
        )
        wordToCountDS.print()

        ssc.start()
        ssc.awaitTermination()

    }
}
