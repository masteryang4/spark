package com.atguigu.bigdata.spark.core.operation.transform
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark12_RDD_Transform1 {

    def main(args: Array[String]): Unit = {

        // 创建Spark上下文环境对象的类称之为Driver类
        // 因为这个类可以驱使整个应用程序执行
        // 所以Driver类的逻辑代码是在Driver端执行的
        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        val list = List(1,2,3,4)
        val rdd = sc.makeRDD(list,2)
        //rdd.saveAsTextFile("output")

        // TODO Spark - 算子 - 转换 - map
        // 分区不变
        // spark中map算子底层会调用scala迭代器的map方法
        // map算子的分区等同于之前的rdd的分区
        val numRDD: RDD[String] = rdd.map(
            num => {
                num * 2 + "s"
            }
        )
        numRDD.saveAsTextFile("output1")
//        println(numRDD.collect().mkString(","))


        sc.stop
    }
}
