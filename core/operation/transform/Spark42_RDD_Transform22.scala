package com.atguigu.bigdata.spark.core.operation.transform
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark42_RDD_Transform22 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)


        // TODO Scala - 转换算子 - combineByKey
        // combineByKey有一个参数列表，三个参数
        // 1. 第一个参数 为了计算的方便，将数据格式进行转换
        // 2. 第二个参数 表示分区内的计算规则
        // 3. 第三个参数 表示分区间的计算规则

        // TODO : 求每个key的平均值 => ( total, cnt )
        val rdd =
            sc.makeRDD(
                List(
                    ("a", 88), ("b", 95), ("a", 91),
                    ("b", 93), ("a", 95), ("b", 98))
                ,2)

        val rdd1: RDD[(String, (Int, Int))] = rdd.combineByKey(
            (x: Int) => (x, 1), // 格式的转换
            (x: (Int, Int), y: Int) => {
                (x._1 + y, x._2 + 1) // 数据相加，数量加1
            },
            (x: (Int, Int), y: (Int, Int)) => {
                (x._1 + y._1, x._2 + y._2) // 数据相加，数量相加
            }
        )
        val resultRDD = rdd1.mapValues(
            t => t._1 / t._2
        )

        resultRDD.collect().foreach(println)

        sc.stop
    }
}
