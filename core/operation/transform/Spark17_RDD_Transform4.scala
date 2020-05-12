package com.atguigu.bigdata.spark.core.operation.transform
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark17_RDD_Transform4 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        val list = List(1,2,3,4)
        val rdd = sc.makeRDD(list,2)

        val rdd1: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex(
            (pindex, datas) => {
                datas.map(
                    (pindex, _)
                )
            }
        )
        rdd1.collect().foreach(println)
        sc.stop
    }
}
