package com.atguigu.bigdata.spark.core.base

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount1 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("wordcount").setMaster("local")
        val sc = new SparkContext(conf)

        sc
            .textFile("input") // 按目录读取所有的文件
            .flatMap(_.split(" "))
            .map( (_,1) )
            .groupBy(_._1)
            .map {
                case (word, list) => {
                    (word, list.size)
                }
            }
            .collect()
            .foreach(println)

        // TODO 3. 释放连接
        sc.stop()
    }
}
