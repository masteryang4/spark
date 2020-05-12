package com.atguigu.bigdata.spark.core.operation.transform
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark44_RDD_Transform23 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)


        // TODO Scala - 转换算子 - sortByKey
        val rdd = sc.makeRDD(List(
            ("a", 1), ("d", 3), ("b", 4), ("c", 2)
        ))

        // sortByKey根据Key来进行排列，默认为升序
        val rdd1 = rdd.sortByKey(true)
        val rdd2 = rdd.sortByKey(false)


        //rdd1.collect().foreach(println)
        rdd2.collect().foreach(println)

        sc.stop
    }
}
