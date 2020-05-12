package com.atguigu.bigdata.spark.core.operation.transform
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark29_RDD_Transform11 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)


        // TODO Scala - 转换算子 - coalesce - 缩减分区
        val rdd = sc.makeRDD(List(1,2,3,4,5,6), 3)

        // 缩减分区，并不会打乱数据，所以可能会导致数据不均衡
        // 如果想要数据均衡一些。应该让数据打乱重新组合在一起

        // 当执行过程中有shuffle操作时，性能会受到影响。执行效率会变低
        val rdd1: RDD[Int] = rdd.coalesce(2)
        val rdd2: RDD[Int] = rdd.coalesce(2, true)

        val rdd3 = rdd2.mapPartitionsWithIndex(
            (index, datas) => {
                datas.map(
                    d => {
                        (index, d)
                    }
                )
            }
        )

        rdd3.collect().foreach(println)



        sc.stop
    }
}
