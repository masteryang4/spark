package com.atguigu.bigdata.spark.core.base
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark09_RDD_Par2 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        // TODO 读取文件数据的分区和读取内存中集合数据的分区是不一样。
        // 读取文件时可以设定最小分区数
        // 关键源码：
        // math.min(defaultParallelism, 2)
        // ==> math.min(8,2) => 2
        //val rdd: RDD[String] = sc.textFile("input/word.txt")
        // TODO 1. 读取文件时，分区计算方式？
        // Spark读取文件底层采用的是Hadoop的切片（分区）规则
        // totalSize = 7byte
        // numSplits = 2
        // long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
        // goalSize = 3
        // 3 + 3 + 1 = 3
        // TODO 2，分解文件数据时，数据如何分区？
        // hadoop读取文件时是按照行的方式的方式读取,一行一行读
        // hadoop读取文件数据时，是按照偏移量读取的
        /*
           0 1 2
           1\n\r

           3 4 5
           2\n\r

           6
           3

         */
        // 0 => (0, 3) => 12
        // 1 => (3, 6) => 3
        // 2 => (6, 7) =>


        val rdd: RDD[String] = sc.textFile("input/word.txt", 2)
        rdd.saveAsTextFile("output")

        sc.stop
    }
}
