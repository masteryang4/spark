package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}

import scala.collection.mutable


object SparkSQL16_Req_Impl2 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
        val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

        // TODO 创建UDAF函数
        val cityRemarkUDAF = new CityRemarkUDAF

        // TODO 注册UDAF函数
        spark.udf.register("cityRemark", cityRemarkUDAF)

        // TODO 链接Hive的Database
        spark.sql("use sparkpractice191125")

        // TODO 1. 连接三张表的数据，获取完整的数据（只有点击）
        spark.sql(
            """
              |select
              |    a.*,
              |    p.product_name,
              |    c.area,
              |    c.city_name
              |from user_visit_action a
              |join product_info p on a.click_product_id = p.product_id
              |join city_info c on c.city_id = a.city_id
              |where a.click_product_id > -1
            """.stripMargin).createOrReplaceTempView("t1")
        // TODO 2. 将数据根据地区，商品名称分组。
        //         使用UDAF函数来聚合城市点击比率
        spark.sql(
            """
              |select
              |   area,
              |   product_name,
              |   count(*) as clickCount,
              |   cityRemark(city_name) as cityRemark
              |from t1 group by area, product_name
            """.stripMargin).createOrReplaceTempView("t2")
        // TODO 3. 统计商品点击次数总和, 取Top3
        spark.sql(
            """
              |select
              |    *,
              |    rank() over ( partition by area order by clickCount desc ) as rank
              |from t2
            """.stripMargin).createOrReplaceTempView("t3")

        spark.sql(
            """
              |select
              |    *
              |from t3
              |where rank <= 3
            """.stripMargin).show

        spark.close

    }

    /**
      * 城市备注聚合函数
      */
    class CityRemarkUDAF extends UserDefinedAggregateFunction {
        // TODO 输入数据的结构类型
        override def inputSchema: StructType = {
            StructType(Array(
                StructField("cityName", StringType)
            ))
        }

        // TODO 中间计算的缓存区的结构类型
        // Long : totalcount
        // Map : (城市1 - count), (城市2 - count), (城市3 - count)
        override def bufferSchema: StructType = {
            StructType(Array(
                StructField("totalcount", LongType),
                StructField("cityToCount", MapType(StringType, LongType))
            ))
        }

        // TODO 计算结果的返回类型
        override def dataType: DataType = StringType

        // TODO 计算稳定性
        override def deterministic: Boolean = true

        // TODO 计算缓冲区的初始化
        override def initialize(buffer: MutableAggregationBuffer): Unit = {
            buffer(0) = 0L
            buffer(1) = Map[String, Long]()
        }

        // TODO 更新缓冲区的值
        override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
            buffer(0) = buffer.getLong(0) + 1

            // 将缓冲区的数据转换为Map类型
            val map = buffer.getAs[Map[String,Long]](1)
            val cityName = input.getString(0)
            val count = map.getOrElse(cityName, 0L)

            //buffer(1) = map
            buffer(1) = map.updated(cityName, count + 1L)
        }

        // TODO 合并缓冲区的值
        override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
            buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)

            val map1 = buffer1.getAs[Map[String,Long]](1)
            val map2 = buffer2.getAs[Map[String,Long]](1)

            buffer1(1) = map1.foldLeft(map2) {
                case ( map, (k, v) ) => {
                    map.updated(k, map.getOrElse(k, 0L) + v)
                    //map
                }
            }
        }

        // TODO 生成备注信息
        override def evaluate(buffer: Row): Any = {
            val map = buffer.getAs[Map[String,Long]](1)
            val totalcnt = buffer.getLong(0)

            // 对统计的城市进行点击量的排序
            val cityToCountList: List[(String, Long)] = map.toList.sortWith(
                (left, right) => {
                    left._2 > right._2
                }
            ).take(2)

            val s = new StringBuilder()
            cityToCountList.foreach {
                case ( city, cnt ) => {
                    s.append(city + " " + (cnt * 100 / totalcnt)   + "%," )
                }
            }
            // 其他
            s.toString()
        }
    }

}
