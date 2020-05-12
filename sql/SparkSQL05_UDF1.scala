package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object SparkSQL05_UDF1 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._
        // TODO 读取JSON数据
        val df: DataFrame = spark.read.json("input/user.json")

        // TODO 读取数据时，DataFrame已经将结构处理好（顺序）
        // 默认的顺序是按照属性的字典顺序
        // userage, username,
        // select username, userage from user
        // 和JDBC处理方式很像
        val ds: Dataset[String] = df.map(
            row => {
                "Name:" + row.getString(1)
            }
        )

        // TODO 创建临时表（视图）
        ds.createTempView("user")

        // TODO 使用SQL的方式访问数据
        // 在sql中使用自己定义的函数
        spark.sql("select * from user").show

        spark.close

    }
    case class Person(name:String, age:Int)
}
