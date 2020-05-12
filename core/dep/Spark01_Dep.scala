package com.atguigu.bigdata.spark.core.dep

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

// Application
object Spark01_Dep {

    def main(args: Array[String]): Unit = {

        // TODO Spark - 依赖关系

        val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")
        val sc = new SparkContext(conf)
//
//        val fileRDD: RDD[String] = sc.textFile("input")
//        println("********** file **********")
//        //println(fileRDD.toDebugString)
//        println(fileRDD.dependencies)
//        val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
//        println("*********** flatMap *********")
//        //println(wordRDD.toDebugString)
//        println(wordRDD.dependencies)
//        val word2OneRDD: RDD[(String, Int)] = wordRDD.map( (_,1) )
//        println("*********** map *********")
//        //println(word2OneRDD.toDebugString)
//        println(word2OneRDD.dependencies)
//        val word2IterRDD: RDD[(String, Iterable[(String, Int)])] = word2OneRDD.groupBy(_._1)
//        println("*********** groupBy *********")
//        //println(word2IterRDD.toDebugString)
//        println(word2IterRDD.dependencies)
//        val word2CountRDD: RDD[(String, Int)] = word2IterRDD.map {
//            case (word, list) => {
//                (word, list.size)
//            }
//        }
//        println("*********** map *********")
//        //println(word2CountRDD.toDebugString)
//        println(word2CountRDD.dependencies)
//        // 一个行动算子，就会产生一个Job
//        val result: Array[(String, Int)] = word2CountRDD.collect()
//        //result.foreach(println)
//        word2CountRDD.collect()


        sc.stop()
    }
}
