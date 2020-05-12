package com.atguigu.bigdata.spark.core.operation.transform
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark30_RDD_test {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)


        // TODO Scala - 转换算子 - coalesce - 扩大分区
        val rdd = sc.makeRDD(List(1,2,3,4,5,6), 3)

        // 默认情况下，coalesce算子不会打乱数据，所有如果扩大分区时，是没有意义的，所以无效
        // 如果非要扩大分区，增加并行度，那么需要打乱并重新组合数据。
        // 所以需要设定第二个参数，shuffle为true
        val rdd1: RDD[Int] = rdd.coalesce(4, true)

        val rdd3 = rdd1.mapPartitionsWithIndex(
            (index, datas) => {
                datas.map(
                    d => {
                        (index, d)
                    }
                )
            }
        )
        //rdd3.saveAsTextFile("output")
        rdd3.collect().foreach(println)



        sc.stop
    }
}
