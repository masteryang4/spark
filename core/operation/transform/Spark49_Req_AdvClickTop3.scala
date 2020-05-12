package com.atguigu.bigdata.spark.core.operation.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark49_Req_AdvClickTop3 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)


        // TODO 需求： 统计出每一个省份每个广告被点击数量排行的Top3

        // TODO 1. 读取数据文件，获取原始数据
        val fileRDD = sc.textFile("input/agent.log")

        // TODO 2. 将读取的数据进行结构的转换
        val mapRDD: RDD[(String, Int)] = fileRDD.map(
            line => {
                val datas = line.split(" ")
                (datas(1) + "-" + datas(4), 1)
            }
        )

        // TODO 3. 将转换结构后的数据进行分组聚合
        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)

        // TODO 4. 将聚合后的数据进行结构的转换
        val mapRDD1: RDD[(String, (String, Int))] = reduceRDD.map {
            case (k, cnt) => {
                val ks = k.split("-")
                (ks(0), (ks(1), cnt))
            }
        }

        // TODO 5. 将转换结构后的数据根据省份进行分组
        val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD1.groupByKey()

        // TODO 6. 将分组后的数据进行排序（降序），取前三名
        // Driver
        val top3RDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
            iter => {
                // Executor端
                // 下面的代码全部都是Scala代码
                iter.toList.sortWith(
                    (left, right) => {
                        left._2 > right._2
                    }
                ).take(3)
            }
        )

        // TODO 7. 将数据采集后打印在控制台
        top3RDD.collect().foreach(println)

        sc.stop
    }
}
