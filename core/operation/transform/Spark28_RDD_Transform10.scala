package com.atguigu.bigdata.spark.core.operation.transform
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark28_RDD_Transform10 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)


        // TODO Scala - 转换算子 - distinct - 数据去重
        val numRDD: RDD[Int] = sc.makeRDD(
            List(1,2,3,4,2,3,4)
        )

        // WordCount
        // ("Hello", "Hello") => ("Hello", 2) => ("Hello")

        // distinct实现源码：
        // map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1)
        val distinctRDD: RDD[Int] = numRDD.distinct()
        // 自定义去重
        //val distinctRDD: RDD[Int] = numRDD.map((_,1)).reduceByKey(_+_).map(_._1)

        println(distinctRDD.collect().mkString(","))


        sc.stop
    }
}
