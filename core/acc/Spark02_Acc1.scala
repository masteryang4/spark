package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Acc1 {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("acc").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)

        // TODO Spark - 累加器
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5))

        // 使用Spark自带的累加器
        // 累加器可以用于累加数据，但是也不仅仅是数值的累加器, 也可以是数据的累加
        // 声明累加器
        val sum = sc.longAccumulator("sum")
        //sc.doubleAccumulator()
        //sc.collectionAccumulator

        rdd.foreach(
            num => {
                // 分布式使用累加器
                // 更新累加器的值
                sum.add(num)
                println("executor:sum=" + sum.value)
            }
        )

        // 读取累加器的值
        println("sum = " + sum.value)

        sc.stop()

    }
}
