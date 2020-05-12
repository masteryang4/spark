package com.atguigu.bigdata.spark.core.operation.transform
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark13_RDD_Transform2 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        val list = List(1,2,3,4)
        val rdd = sc.makeRDD(list,1)

        // 因为数据是有多个分区，所以可以并行计算
        // 同一个分区的数据应该是迭代执行
        val numRDD: RDD[String] = rdd.map(
            num => {
                println("num = " + num)
                num * 2 + "s"
            }
        )
        val strRDD: RDD[String] = numRDD.map(
            s => {
                println("string = " + s)
                s * 2
            }
        )
        //println(strRDD.collect().mkString(","))
        strRDD.saveAsTextFile("output")
        sc.stop
    }
}
