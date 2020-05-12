package com.atguigu.bigdata.spark.sql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._


object SparkSQL10_Load_Mysql {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._
        // TODO 读取MySQL数据
        val mysqlDF: DataFrame = spark.read.format("jdbc")
                .option("url", "jdbc:mysql://linux1:3306/rdd")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("user", "root")
                .option("password", "000000")
                .option("dbtable", "user")
                .load()


//        val prop = new Properties()
//        prop.setProperty("user", "root")
//        prop.setProperty("password", "000000")
//        prop.setProperty("driver", "com.mysql.jdbc.Driver")
//        spark.read.jdbc("jdbc:mysql://linux1:3306/rdd", "user", prop)

        spark.close

    }

}
