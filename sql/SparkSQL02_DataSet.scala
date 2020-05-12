package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object SparkSQL02_DataSet {

    def main(args: Array[String]): Unit = {

        // TODO 创建配置信息
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")

        // TODO 创建上下文环境对象
        // new SparkSession(X)
        // SparkSession.apply(X)
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        // TODO 导入隐式转换规则
        // TODO 无论是否用到隐式转换，都需要增加
        import spark.implicits._

        // TODO DataSet
        val list = List(
            Person( "zhangsan", 30 ),
            Person( "lisi", 40 ),
            Person( "wangwu", 50 )
        )
        val ds: Dataset[Person] = list.toDS

        // Dataset的功能使用方式和DataFrame是一样的。
        // 所谓的DataFrame其实就是特定类型的Dataset的别名，所以功能一样。
        //ds.createTempView()
        //ds.createGlobalTempView()
        //ds.filter()
        //ds.groupBy()
        //ds.select()


        // TODO 关闭环境对象
        spark.close

    }
    case class Person(name:String, age:Int)
}
