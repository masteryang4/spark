package com.atguigu.bigdata.spark.core.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_Par3 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        // 3 + 3 => 6byte
        // 6 / 3 = 2
        // 2 + 2 + 2 = 3
        // 3 / 2 + 3 / 2 =
        // 1 + 1 + 1 + 1 = 4
        // hadoop计算分区数量时按照所有文件的总的字节数
        // 但是真正进行分区的时候，是按照文件来分区

        val rdd: RDD[String] = sc.textFile("input", 3)
        rdd.saveAsTextFile("output")

        sc.stop
    }
}
