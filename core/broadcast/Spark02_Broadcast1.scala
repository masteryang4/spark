package com.atguigu.bigdata.spark.core.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks

object Spark02_Broadcast1 {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("broadcast").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)

        val rdd1 = sc.makeRDD(
            List(
                ("a", 1), ("b", 2), ("c", 3)
            )
        )
        val list =
            List(
                ("a", 4), ("b", 5), ("c", 6)
            )

        // TODO 声明广播变量
        val listBroadcast: Broadcast[List[(String, Int)]] = sc.broadcast(list)

        // ("a", 1), ("b", 2), ("c", 3)
        // =>
        // ("a", (1,4)), ("b", (2,5)), ("c", (3,6))
        val mapRDD =
            rdd1.map{
                case ( k, v ) => {
                    var otherv = 0
                    Breaks.breakable{
                        // TODO 使用广播变量
                        for ( (k1, v1) <- listBroadcast.value ) {
                            if ( k == k1 ) {
                                otherv = v1
                                Breaks.break()
                            }
                        }
                    }
                    (k, (v, otherv))
                }
            }

        mapRDD.foreach(println)

        sc.stop

    }
}
