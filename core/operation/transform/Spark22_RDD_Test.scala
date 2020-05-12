package com.atguigu.bigdata.spark.core.operation.transform
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark22_RDD_Test {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        // TODO : 计算所有分区最大值求和
        val list = List(1,4,2,8)

        val rdd = sc.makeRDD(list,2)

        // 将分区数据封装到数组当中
        val glomRDD: RDD[Array[Int]] = rdd.glom()

        // 将封装后的数组进行结构的转换 array => max
        val maxRDD: RDD[Int] = glomRDD.map(
            array => array.max
        )
        // 将数据采集回来进行统计（求和）
        val ints: Array[Int] = maxRDD.collect()
        println(ints.sum)

        sc.stop
    }
}
