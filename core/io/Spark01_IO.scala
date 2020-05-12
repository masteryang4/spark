package com.atguigu.bigdata.spark.core.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
//import org.mortbay.util.ajax.JSON

object Spark01_IO {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)

        val jsonRDD: RDD[String] = sc.textFile("input/user.json")
        //sc.sequenceFile()
        //sc.objectFile()

        // 1. Scala中解析类有问题
        // 2. Spark读取文件是一行一行读取的。而JSON文件无法保证一行数据就是一个JSON
        //    所以Spark读取的JSON文件应该要求一行就是一个JSON对象
        // 3. Spark Core解析JSON文件，其实等同于解析成K-V对象，一般情况下不这么使用
        //    一般采用Spark SQL来访问。
        //val resultRDD: RDD[Option[Any]] = jsonRDD.map(JSON.parse)
        jsonRDD.foreach(println)

        sc.stop()
    }
}
