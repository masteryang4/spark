package com.atguigu.bigdata.spark.core.operation.transform
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark18_RDD_Test {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        val rdd = sc.makeRDD( List(1,2,3,4), 2 )

        // TODO : 获取第二个数据分区的数据
        // 获取的分区号是从0开始的
        val rdd1: RDD[Int] = rdd.mapPartitionsWithIndex(
            (index, datas) => {
                if ( index == 1 ) {
                    datas
                } else {
                    Nil.iterator
                }
            }
        )
        println(rdd1.collect().mkString(","))

        sc.stop
    }
}
