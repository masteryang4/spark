package com.atguigu.bigdata.spark.core.operation.transform
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark41_RDD_Transform21 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)


        // TODO Scala - 转换算子 - foldByKey
        // 如果aggregateByKey算子中分区内计算规则和分区间计算规则相同的话
        // 那么可以采用其他算子来代替

        val rdd =
            sc.makeRDD(List(
                ("a",1),("a",2),("c",3),
                ("b",4),("c",5),("c",6)
            ),2)

        //reduceByKey(_+_)
        val resultRDD = rdd.foldByKey(0)(_+_)


        resultRDD.collect().foreach(println)

        sc.stop
    }
}
