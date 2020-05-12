package com.atguigu.bigdata.spark.core.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Collection {

    def main(args: Array[String]): Unit = {

        // TODO 从集合中创建RDD
        // master => spark环境
        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)


        // TODO 1. parallelize ： 并行
        val list = List(1,2,3,4)
        // RDD是一种数据结构，表示如果数据读取过来后，该如何进行处理和封装.
        // RDD其实并不会保存数据，只是封装了处理逻辑和基本的结构属性
        val numRDD: RDD[Int] = sc.parallelize(list)
        println(numRDD.collect().mkString(","))

        // TODO 2. makeRDD
        // makeRDD底层代码就是调用了parallelize方法，
//        val numRDD1: RDD[Int] = sc.makeRDD(list)
//        println(numRDD1.collect().mkString(","))
        sc.stop
    }
}
