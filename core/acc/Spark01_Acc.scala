package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("acc").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)

        // 求和
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5),1)
//        val i: Int = rdd.reduce(_+_)
//        val d: Double = rdd.sum()
//        println(d)
        var sum = 0
        // 分布式循环
        rdd.foreach(
            num => {
                sum = num + sum
            }
        )
        println("sum = " + sum)


        sc.stop()

    }
}
