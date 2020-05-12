package com.atguigu.bigdata.spark.core.operation.transform
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark23_RDD_Transform7 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        val numRDD = sc.makeRDD(
            List(1,2,3,4)
        )

        // groupBy会按照指定的规则对数据进行分组
        // 指定的计算规则用于计算分组key，当key相同时，会将数据放在一起
        // 0 => [2,4]
        // 1 => [1,3]
        // 分组后的结果是元组类型
        // 元组的第一个元素是分组key
        // 元组的第二个元素是相同key的数据集合
        val groupRDD: RDD[(Int, Iterable[Int])] = numRDD.groupBy(num => num % 2)

        groupRDD.collect().foreach(println)

        sc.stop
    }
}
