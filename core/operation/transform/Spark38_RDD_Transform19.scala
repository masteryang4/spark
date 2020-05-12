package com.atguigu.bigdata.spark.core.operation.transform
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark38_RDD_Transform19 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)


        // TODO Scala - 转换算子 - groupByKey
        var rdd = sc.makeRDD(
            List(
                ("hello", 1),
                ("hello", 2),
                ("hadoop", 2)
            )
        )

        // 使用key进行分组操作
        val rdd1: RDD[(String, Iterable[Int])] = rdd.groupByKey()

        val rdd2 = rdd1.mapValues(
            datas => {
                datas.sum
            }
        )

        rdd2.collect().foreach(println)

        sc.stop
    }
}
