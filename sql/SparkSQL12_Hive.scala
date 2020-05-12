package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._


object SparkSQL12_Hive {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
        val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
        import spark.implicits._

        // TODO 访问外置的Hive
        spark.sql("show tables").show


        spark.close

    }

}
