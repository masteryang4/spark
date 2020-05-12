package com.atguigu.bigdata.spark.core.operation.transform
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark26_RDD_Test {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        // TODO : 从服务器日志数据apache.log中获取2015年5月17日的请求路径
        val logRDD = sc.textFile("input/apache.log")

        val timeRDD = logRDD.map(
            log => {
                val datas = log.split(" ")
                datas(3)
            }
        )

        val filterRDD: RDD[String] = timeRDD.filter(
            time => {
                val ymd = time.substring(0, 10)
                ymd == "17/05/2015"
            }
        )
        filterRDD.collect().foreach(println)

        sc.stop
    }
}
