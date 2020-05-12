package com.atguigu.bigdata.spark.core.operation.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Action5 {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        // TODO Spark - RDD - 行动算子
        val rdd = sc.makeRDD(List(1,2,3,4),2)

        rdd.collect().foreach(println) // 内存循环打印
        println("*********************")
        // 算子
        // Drvier
        rdd.foreach(
            str => {
                // Executor
                println(str)// 分布式循环打印
            }
        )

        sc.stop

    }
}
