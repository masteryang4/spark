package com.atguigu.bigdata.spark.core.operation.transform
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark46_RDD_Transform24 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)


        // TODO Scala - 转换算子 - join
        val rdd1 = sc.makeRDD(
            List(
                ("a",1), ("b",2),("a",3)
            )
        )
        val rdd2 = sc.makeRDD(
            List(
                ("b",4),("a",5)
            )
        )

        // join 条件是相同的key会将value连接在一起
        // join底层实现有一个类似于笛卡尔乘积的概念, 所以性能比较差
        val result: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
        //val zipRDD: RDD[((String, Int), (String, Int))] = rdd1.zip(rdd2)

        result.collect().foreach(println)
        println("************************")
        //zipRDD.collect().foreach(println)

        sc.stop
    }
}
