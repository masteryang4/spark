package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.util.AccumulatorV2


object SparkSQL08_UDAF_DIY {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        // TODO 读取JSON数据
        val df: DataFrame = spark.read.json("input/user.json")

        // TODO 使用自定义聚合函数实现年龄的平均值计算
        // buffer
        // select avg(age) from user
        // 创建自定义函数
        // 函数类
        val udaf = new MyAvgAgeUDAF
        // 注册UDAF函数
        spark.udf.register("avgAge", udaf)

        df.createTempView("user")

        spark.sql("select avgAge(userage) from user").show


        spark.close

    }
    /*
     * TODO 自定义聚合函数（UDAF）
     * 1. 继承UserDefinedAggregateFunction
     * 2. 重写方法
     */
    class MyAvgAgeUDAF extends UserDefinedAggregateFunction {
        // TODO 传入聚合函数的数据结构
        // 1 => age => Long
        override def inputSchema: StructType = {
            StructType(Array(
                StructField("age", LongType)
            ))
        }

        // TODO 用于计算的缓冲区的数据结构
        override def bufferSchema: StructType = {
            StructType(Array(
                StructField("totalage", LongType),
                StructField("totalcnt", LongType)
            ))
        }

        // TODO 输出结果的类型
        override def dataType: DataType = DoubleType

        // TODO 函数稳定性（幂等性）
        // 给函数相同的输入值，计算结果也相同
        override def deterministic: Boolean = true

        // TODO 用于计算的缓冲区初始化
        override def initialize(buffer: MutableAggregationBuffer): Unit = {
            buffer(0) = 0L
            buffer(1) = 0L
        }

        // TODO 将输入的值更新到缓冲区中
        override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
            buffer(0) = buffer.getLong(0) + input.getLong(0)
            buffer(1) = buffer.getLong(1) + 1L
        }

        // TODO 合并缓冲区
        // MutableAggregationBuffer 继承了Row
        override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
            buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
            buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
        }

        // TODO 计算结果
        override def evaluate(buffer: Row): Any = {
            buffer.getLong(0).toDouble / buffer.getLong(1)
        }
    }

}
