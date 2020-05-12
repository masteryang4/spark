package com.atguigu.bigdata.spark.core.operation.transform
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark34_RDD_Transform15 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)


        // TODO Scala - 转换算子 - zip

        // spark中的拉链操作必须要求两个RDD分区数量一致
        // 并且每个分区的元素数量是一致的。
        // 否则无法拉链，会出现错误。
        val rdd1 = sc.makeRDD(List(1,2,3,4,5),2)
        val rdd2 = sc.makeRDD(List(3,4,5,6),2)
        val rdd3 = sc.makeRDD(List("3","4","5","6"))

        val zipRDD: RDD[(Int, Int)] = rdd1.zip(rdd2)
        //val zipRDD: RDD[(Int, String)] = rdd1.zip(rdd3)

        println(zipRDD.collect().mkString(","))

        sc.stop
    }
}
