package com.atguigu.bigdata.spark.core.operation.transform
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark21_RDD_Transform6 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        val list = List(1,2,3,4)
        val rdd = sc.makeRDD(list,2)

        val rdd1: RDD[Array[Int]] = rdd.glom()

        rdd1.collect().foreach(
            array => {
                println(array.mkString(","))
            }
        )

        sc.stop
    }
}
