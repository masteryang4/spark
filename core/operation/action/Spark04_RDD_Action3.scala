package com.atguigu.bigdata.spark.core.operation.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Action3 {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        // TODO Spark - RDD - 行动算子
        val rdd = sc.makeRDD(List("Hello","World","Hello","Scala"))

        // WordCount
        //val stringToLong: collection.Map[String, Long] = rdd.map((_,1)).countByKey()

        // WordCount
        val stringToLong: collection.Map[String, Long] = rdd.countByValue()

        println(stringToLong)

        sc.stop

    }
}
