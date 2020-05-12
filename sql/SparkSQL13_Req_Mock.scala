package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._


object SparkSQL13_Req_Mock {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
        val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
        import spark.implicits._
        // TODO 向Hive创建表并插入数据
        // 如果在Hive命令行窗口中，需要使用分号
        // 如果在程序中执行sql，不需要分号结尾
        spark.sql("use sparkpractice191125")

        spark.sql(
            """
              |CREATE TABLE `user_visit_action`(
              |  `date` string,
              |  `user_id` bigint,
              |  `session_id` string,
              |  `page_id` bigint,
              |  `action_time` string,
              |  `search_keyword` string,
              |  `click_category_id` bigint,
              |  `click_product_id` bigint,
              |  `order_category_ids` string,
              |  `order_product_ids` string,
              |  `pay_category_ids` string,
              |  `pay_product_ids` string,
              |  `city_id` bigint)
              |row format delimited fields terminated by '\t'
            """.stripMargin)

        spark.sql(
            """
              |load data local inpath 'input/user_visit_action.txt' into table sparkpractice191125.user_visit_action
            """.stripMargin)

        spark.sql(
            """
              |CREATE TABLE `product_info`(
              |  `product_id` bigint,
              |  `product_name` string,
              |  `extend_info` string)
              |row format delimited fields terminated by '\t'
            """.stripMargin)

        spark.sql(
            """
              |load data local inpath 'input/product_info.txt' into table sparkpractice191125.product_info
            """.stripMargin)

        spark.sql(
            """
              |CREATE TABLE `city_info`(
              |  `city_id` bigint,
              |  `city_name` string,
              |  `area` string)
              |row format delimited fields terminated by '\t'
            """.stripMargin)

        spark.sql(
            """
              |load data local inpath 'input/city_info.txt' into table sparkpractice191125.city_info
            """.stripMargin)

        spark.sql(
            """
              |select * from city_info
            """.stripMargin).show(10)

        spark.close

    }

}
