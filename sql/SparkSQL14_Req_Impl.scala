package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._


object SparkSQL14_Req_Impl {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
        val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
        import spark.implicits._

        // TODO 链接Hive的Database
        spark.sql("use sparkpractice191125")

        // TODO 1. 连接三张表的数据，获取完整的数据（只有点击）
        // TODO 2. 将数据根据地区，商品名称分组。
        // TODO 3. 统计商品点击次数总和
        // TODo 4. 使用UDAF函数来聚合城市点击比率

        spark.sql(
            """
              |        select
              |            *
              |        from (
              |            select
              |                *,
              |                rank() over ( partition by area order by clickCount desc ) as rank
              |            from (
              |                select
              |                   area,
              |                   product_name,
              |                   count(*) as clickCount
              |                from (
              |
              |                    select
              |                        a.*,
              |                        p.product_name,
              |                        c.area,
              |                        c.city_name
              |                    from user_visit_action a
              |                    join product_info p on a.click_product_id = p.product_id
              |                    join city_info c on c.city_id = a.city_id
              |                    where a.click_product_id > -1
              |
              |                ) t1 group by area, product_name
              |            ) t2
              |        ) t3
              |        where rank <= 3
            """.stripMargin).show



        spark.close

    }

}
