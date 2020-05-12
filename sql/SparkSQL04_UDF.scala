package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object SparkSQL04_UDF {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        // TODO 读取JSON数据
        val df: DataFrame = spark.read.json("input/user.json")

        // 自定义数据处理函数（针对于某一列），称之为UDF
        spark.udf.register("changeName",(name:String)=> {
            "姓名 ：" + name
        })
        spark.udf.register("descAge", (age:Int) => {
            "年龄 ：" + age
        })

        // TODO 创建临时表（视图）
        df.createTempView("user")

        // TODO 使用SQL的方式访问数据
        // 在sql中使用自己定义的函数
        spark.sql("select changeName(username), descAge(userage) from user").show

        spark.close

    }
    case class Person(name:String, age:Int)
}
