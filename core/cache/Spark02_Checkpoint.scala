package com.atguigu.bigdata.spark.core.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Checkpoint {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)
        // 设置检查点路径， 一般路径应该为分布式存储路径，HDFS
        sc.setCheckpointDir("cp")

        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

        // TODO 检查点

        // RDD的持久化可能会导致数据丢失，如果数据丢失，那么需要重新再次计算，性能不高
        // 所以如果能够保证数据不丢，那么是一个好的选择
        // 可以将数据保存到检查点中，这样是分布式存储，所以比较安全。
        // 所以将数据保存到检查点前，需要设定检查点路径
        val rdd1 = rdd.map(
            num => {
                //println("num.....")
                num
            }
        )

        // 检查点
        // 检查点为了准确，需要重头再执行一遍，就等同于开启一个新的作业
        // 为了提高效率，一般情况下，是先使用cache后在使用检查点

        // 检查点会切断RDD的血缘关系。将当前检查点当成数据计算的起点。
        // 持久化操作是不能切断血缘关系，因为一旦内存中数据丢失，无法恢复数据
        val rdd2: RDD[Int] = rdd1.cache()
        rdd2.checkpoint()
        println(rdd2.toDebugString)
        println(rdd2.collect().mkString(","))
        println(rdd2.toDebugString)
        println("**********************")
        println(rdd2.collect().mkString(","))

        sc.stop()
    }
}
