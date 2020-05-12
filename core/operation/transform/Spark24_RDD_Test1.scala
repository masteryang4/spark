package com.atguigu.bigdata.spark.core.operation.transform
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark24_RDD_Test1 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        // TODO : 从服务器日志数据apache.log中获取每个时间段访问量
        val logRDD = sc.textFile("input/apache.log")

        val timeRDD: RDD[String] = logRDD.map(
            log => {
                val datas = log.split(" ")
                datas(3)
            }
        )

        // 将时间按照小时进行分组
        val hourRDD: RDD[(String, Iterable[String])] = timeRDD.groupBy(
            time => {
                time.substring(11, 13)
            }
        )

        // 如果处理数据时，key保持不变，仅仅对value做操作，那么可以采用一个特殊的方式来实现
        // TODO mapValues
        val result = hourRDD.mapValues(list=>list.size)

//        val result =
//            hourRDD.map{
//                case ( hour, list ) => {
//                    (hour, list.size)
//                }
//            }

        result.collect().foreach(println)

        sc.stop
    }
}
