package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset, Row, SparkSession}


object SparkSQL01_DataFrame {

    def main(args: Array[String]): Unit = {

        // TODO 创建配置信息
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")

        // TODO 创建上下文环境对象
        // new SparkSession(X)
        // SparkSession.apply(X)
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        // TODO DataFrame
        // 读取JSON文件，要求JSON文件中的每一行数据符合JSON的格式
        // DataFrame类型应该是一个别名
        val df: DataFrame = spark.read.json("input/user.json")

        // TODO 使用SQL的语法访问数据
        //df.createTempView("user")
        //df.createGlobalTempView("guser")
        // 展示数据
        //spark.sql("select avg(userage)  from user").show
        //spark.sql("select * from global_temp.guser").show
        //spark.newSession().sql("select * from global_temp.guser").show

        // TODO 使用DSL语法访问数据
        // 需要导入隐式转换规则:
        // 这里的spark不是包的名称，表示SparkSQL上下文环境对象变量的名称
        // TODO import 可以导入val变量内部的属性，方法。var是不可以的。
        import spark.implicits._
        //df.select("username", "userage").show
        //df.select($"username", $"userage").show
        //df.select('userage).show

        //df.filter()
        //df.map()
        //df.flatMap()
        //df.mapPartitions()
        //df.groupBy().count()


        // RDD, DataFrame, Dataset


        // TODO 关闭环境对象
        spark.close

    }
}
