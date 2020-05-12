package com.atguigu.bigdata.spark.core.operation.transform
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark47_RDD_Transform25 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)


        // TODO Scala - 转换算子 - leftOuterJoin
        val rdd1 = sc.makeRDD(
            List(
                ("a",1), ("b",2), ("c",3)
            )
        )
        val rdd2 = sc.makeRDD(
            List(
                ("a",4), ("b",5)
            )
        )

        val result: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)

        val result1: RDD[(String, (Option[Int], Int))] = rdd2.rightOuterJoin(rdd1)

        result1.collect().foreach(println)
        println("***************************")
        result1.collect().foreach(println)

        // Option : 选项类型 ，可有可无
        // 有值：Some
        // 无值：None

        sc.stop
    }
}
