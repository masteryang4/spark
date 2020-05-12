package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession, TypedColumn}


object SparkSQL09_UDAF_DIY_Class {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._
        // TODO 读取JSON数据
        val df: DataFrame = spark.read.json("input/user.json")

        // TODO 创建自定义函数对象
        val udaf = new MyAvgAgeUDAFClass

        // TODO 将聚合函数转换为查询的列
        val column: TypedColumn[User, Double] = udaf.toColumn

        // TODO 创建临时表
        //df.createTempView("user")
        // SQL 面向的是二维表格
        // SQL中不支持强类型语法操作，所以不支持强类型的聚合函数
        //spark.sql("select avgAge(userage) from user").show

        // 强类型的聚合函数需要使用DSL语法访问
        // 所以一般采用Dataset进行调用
        val ds: Dataset[User] = df.as[User]
        ds.select(column).show

        spark.close

    }
    case class User(username:String, userage:Long)
    case class AvgBuffer( var sumage:Long, var sumcnt:Long )
    // 自定义聚合函数 （UDAF） - 强类型
    // 1. 继承Aggregator，定义泛型
    //    1.1  IN ：输入数据的类型
    //    1.2. BUF ：中间处理时缓存类型
    //    1.3  OUT ：计算结果时的输出类型
    // 2. 重写方法
    class MyAvgAgeUDAFClass extends Aggregator[User, AvgBuffer, Double]{

        // TODO 缓存的初始化
        override def zero: AvgBuffer = {
            AvgBuffer(0L,0L)
        }

        // TODO 聚合数据
        override def reduce(buffer: AvgBuffer, user: User): AvgBuffer = {
//            buffer.sumage = buffer.sumage + user.userage
//            buffer.sumcnt = buffer.sumcnt + 1
//            buffer
            AvgBuffer( buffer.sumage + user.userage, buffer.sumcnt + 1 )
        }

        // TODO 合并缓冲区
        override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
            AvgBuffer( b1.sumage + b2.sumage, b1.sumcnt + b2.sumcnt )
        }

        // TODO 聚合函数处理结果
        override def finish(reduction: AvgBuffer): Double = {
            reduction.sumage.toDouble / reduction.sumcnt
        }

        // TODO 缓冲区类型的编码
        override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

        // TODO 返回结果的编码
        override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
    }

}
