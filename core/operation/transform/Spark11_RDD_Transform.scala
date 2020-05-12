package com.atguigu.bigdata.spark.core.operation.transform
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark11_RDD_Transform {

    def main(args: Array[String]): Unit = {

        // 创建Spark上下文环境对象的类称之为Driver类
        // 因为这个类可以驱使整个应用程序执行
        // 所以Driver类的逻辑代码是在Driver端执行的
        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        val list = List(1,2,3,4)
        // map方法在调用时会触发执行
//        val list1 = list.map(
//            num => {
//                println("map....")
//                num * 2 + "s"
//            }
//        )
//        println(list1)

        val rdd = sc.makeRDD(list)

        // TODO Spark - 算子 - 转换 - map

        // 将数据进行转换，这里的转换可以是数值变化，也可以是类型变化
        // map算子的执行不会马上执行，而是延迟执行。
        // map方法中的逻辑是在executor端执行
        // 因为map外部的代码是执行在driver端，而map内部的代码逻辑执行executor端
        // 所以为了区分map和其他scala方法，所以将rdd的map方法称之为算子
        val numRDD: RDD[String] = rdd.map(
            num => {
                println("map...")
                num * 2 + "s"
            }
        )
        println("xxxxx")
        println(numRDD.collect().mkString(","))


        sc.stop
    }
}
