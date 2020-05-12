package com.atguigu.bigdata.spark.core.operation.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Action2 {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        // TODO Spark - RDD - 行动算子

        // aggregate
        // aggregate初始值在分区内计算时会使用，分区间计算也会使用
        val rdd = sc.makeRDD(List(1,2,3,4),2)

        //rdd.aggregate(0)(math.max(_, _), _+_)
        // 10, 1, 2
        //   10  2
        //      10
        //          ==> 10 + 10 + 10 => 30
        //      10
        //    10  4
        // 10, 3, 4
        val i: Int = rdd.aggregate(10)(
            (x, y) => math.max(x, y),
            (x, y) => x + y
        )

        // 10,1,2
        //   11, 2
        //     13
        //         => 10 + 13 + 17 = 40
        //     17
        //   13, 4
        // 10,3,4
        //rdd.aggregate(10)(_+_, _+_)
        val j = rdd.fold(10)(_+_)

        println("i = " + i)
        println("j = " + j)

        sc.stop

    }
}
