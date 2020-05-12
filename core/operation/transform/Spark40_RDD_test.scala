package com.atguigu.bigdata.spark.core.operation.transform
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark40_RDD_test {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)


        // TODO Scala - 转换算子 - aggregateByKey
        // TODO : 分区内计算规则和分区间计算规则相同怎么办
        val rdd = sc.makeRDD(
            List(
                ("a",1),("a",2),("c",3),
                ("b",4),("c",5),("c",6)
            ),
            2
        )

        val result = rdd.aggregateByKey(0)(_+_,_+_)
//            rdd.aggregateByKey(0)(
//                (x, y) => x + y,
//                (x, y) => x + y
//            )

        result.collect().foreach(println)


        sc.stop
    }
}
