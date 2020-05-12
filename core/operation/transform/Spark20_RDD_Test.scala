package com.atguigu.bigdata.spark.core.operation.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark20_RDD_Test {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        val rdd = sc.makeRDD( List(
            List(1,2),
            3,
            List(4,5)), 1 )

        val rdd1 = rdd.flatMap(
            data => {
                data match {
                    case list:List[_] => list
                    case d => List(d)
                }
            }
        )

        rdd1.collect().foreach(println)


        sc.stop
    }
}
