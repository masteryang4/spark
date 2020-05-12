package com.atguigu.bigdata.spark.core.operation.transform
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark25_RDD_Transform8 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        val numRDD = sc.makeRDD(
            List(1,2,3,4)
        )

        // TODO Scala - 转换算子 - filter
        val filterRDD = numRDD.filter(num=>num%2 == 0)

        filterRDD.collect().foreach(println)

        sc.stop
    }
}
