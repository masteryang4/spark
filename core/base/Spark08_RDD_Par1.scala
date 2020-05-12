package com.atguigu.bigdata.spark.core.base
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark08_RDD_Par1 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        //conf.set("spark.default.parallelism", "4")

        val sc = new SparkContext(conf)

        // 1 => 0
        // 2 => 1
        // 3 => 1
        // 4 => 2
        // 5 => 2
        // 数据分区的源码：
        /*
        def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
          (0 until numSlices).iterator.map { i =>
            val start = ((i * length) / numSlices).toInt
            val end = (((i + 1) * length) / numSlices).toInt
            (start, end)
          }
        }
        。。。
        0 => (0, 1) => 1
        1 => (1, 2) => 2
        2 => (2, 4) => 3, 4

        0 => (0, 1) => 1
        1 => (1, 3) => 2, 3
        2 => (3, 5) => 4, 5
        array.slice(start, end).toSeq
         */
        //Array(1,2,3,4).slice()
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),3)

        rdd.saveAsTextFile("output")

        sc.stop
    }
}
