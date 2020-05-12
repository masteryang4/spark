package com.atguigu.bigdata.spark.core.broadcast

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Broadcast {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("broadcast").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)

        val rdd1 = sc.makeRDD(
            List(
                ("a", 1), ("b", 2), ("c", 3)
            )
        )
        val rdd2 = sc.makeRDD(
            List(
                ("a", 4), ("b", 5), ("c", 6)
            )
        )

        // ("a", (1,4)), ("b", (2,5)), ("c", (3,6))
        val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

        joinRDD.foreach(println)

        sc.stop

    }
}
