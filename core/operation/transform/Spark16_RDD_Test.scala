package com.atguigu.bigdata.spark.core.operation.transform
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark16_RDD_Test {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        val rdd = sc.makeRDD( List(1,5,7,4), 2 )

        // TODO : 获取每个数据分区的最大值
        val maxRDD: RDD[Int] = rdd.mapPartitions(
            datas => {
                List(datas.max).iterator
            }
        )
        println(maxRDD.collect().mkString(","))

        sc.stop
    }
}
