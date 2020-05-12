package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._


object SparkSQL11_Save_Mysql {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        // TODO 保存MySQL数据
        val mysqlDF: DataFrame = spark.read.format("jdbc")
                .option("url", "jdbc:mysql://linux1:3306/rdd")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("user", "root")
                .option("password", "000000")
                .option("dbtable", "user")
                .load()

        mysqlDF.write
                .format("jdbc")
                .option("url", "jdbc:mysql://linux1:3306/rdd")
                .option("user", "root")
                .option("password", "000000")
                .option("dbtable", "user1")
                //.mode("append")
                .mode(SaveMode.Append)
                .save()
        spark.close

    }

}
