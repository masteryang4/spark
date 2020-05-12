package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.util.AccumulatorV2


object SparkSQL07_UDAF_Acc {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        // TODO 读取JSON数据
        val df: DataFrame = spark.read.json("input/user.json")

        // TODO 使用累加器获取年龄的平均值
        // TODO 创建累加器
        val acc = new MyAvgAgeAcc
        // TODO 注册累加器
        spark.sparkContext.register(acc, "avgAge")
        // TODO 使用累加器
        df.foreach(
            row => {
                acc.add(row.getLong(0))
            }
        )

        // TODO 获取累加器的值
        val ( totalage, totalcnt ) = acc.value

        // TODO 计算年龄的平均值
        println(totalage.toDouble / totalcnt)


        spark.close

    }

    /**
      * 自定义累加器
      * 1. 继承AccumulatorV2，定义泛型
      *    1.1 IN
      *    1.2 OUT
      * 2. 重写方法
      */
    class MyAvgAgeAcc extends AccumulatorV2[Long, (Long, Long)] {

        var buffer = (0L, 0L)

        override def isZero: Boolean = {
            buffer._1 == 0L && buffer._2 == 0L
        }

        override def copy(): AccumulatorV2[Long, (Long, Long)] = {
            new MyAvgAgeAcc
        }

        override def reset(): Unit = {
            buffer = (0L, 0L)
        }

        override def add(v: Long): Unit = {
            buffer = ( buffer._1 + v, buffer._2 + 1 )
        }

        override def merge(other: AccumulatorV2[Long, (Long, Long)]): Unit = {
            val ( oage, ocnt ) = other.value
            buffer = ( buffer._1 + oage, buffer._2 + ocnt )
        }

        override def value: (Long, Long) = buffer
    }
}
