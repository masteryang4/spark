package com.atguigu.bigdata.spark.core.serial

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark01_Serial {

    def main(args: Array[String]): Unit = {
        //1.创建SparkConf并设置App名称
        val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

        //2.创建SparkContext，该对象是提交Spark App的入口
        val sc: SparkContext = new SparkContext(conf)

        //3.创建一个RDD
        val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

        //3.1创建一个Search对象
        val search = new Search("hello")

        //val matchRDD: RDD[String] = search.getMatch1(rdd)
        val matchRDD: RDD[String] = search.getMatch2(rdd)

        matchRDD.foreach(println)


        sc.stop()
    }
    class Search(query:String) extends Serializable {

        def isMatch(s: String): Boolean = {
            s.contains(query)
        }

        // 函数序列化案例
        def getMatch1 (rdd: RDD[String]): RDD[String] = {
            rdd.filter(this.isMatch)
            rdd.filter(isMatch)
        }

        // 属性序列化案例
        def getMatch2(rdd: RDD[String]): RDD[String] = {
            //rdd.filter(x => x.contains(this.query))
            rdd.filter(x => x.contains(query))
            //val q = query
            //rdd.filter(x => x.contains(q))
        }
    }
}
