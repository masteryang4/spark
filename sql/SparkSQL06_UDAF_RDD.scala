package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object SparkSQL06_UDAF_RDD {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._
        // TODO 读取JSON数据
        val df: DataFrame = spark.read.json("input/user.json")

        // TODO 使用RDD获取年龄的平均值
        val rdd: RDD[Row] = df.rdd
        val ageToOneRDD: RDD[(Long, Int)] = rdd.map(
            row => {
                (row.getLong(0), 1)
            }
        )
        val ageToCount: (Long, Int) = ageToOneRDD.reduce(
            (t1, t2) => {
                (t1._1 + t2._1, t1._2 + t2._2)
            }
        )
        println("平均年龄 = " + ageToCount._1 / ageToCount._2.toDouble)


        spark.close

    }
    case class Person(name:String, age:Int)
}
