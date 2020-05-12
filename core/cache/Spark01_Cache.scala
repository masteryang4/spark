package com.atguigu.bigdata.spark.core.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Cache {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)

        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

        // TODO 持久化

        // 1. 如果RDD执行过程中，出现了错误，应该从头执行，但是中间过程可能非常慢
        //    为了避免重复执行，可以将比较重要的数据，和数据操作比较耗时的处理使用持久化保存起来。
        //    如果保存起来后，那么一旦发生错误，spark可以从持久化的位置重新执行。提高效率
        // 2. 重复使用，提高效率
//        rdd.persist(StorageLevel.MEMORY_ONLY)
//        rdd.persist(StorageLevel.DISK_ONLY)

        // 3 .缓存操作是在行动算子执行之后产生的。

        val rdd1 = rdd.map(
            num => {
                println("num.....")
                num
            }
        )

        //val cacheRDD: RDD[Int] = rdd1.persist(StorageLevel.MEMORY_ONLY)
        println(rdd1.toDebugString)
        val cacheRDD: RDD[Int] = rdd1.cache()
        println(cacheRDD.toDebugString)
        println(cacheRDD.collect().mkString(","))
        println("**********************")
        cacheRDD.foreach(println)

        sc.stop()
    }
}
