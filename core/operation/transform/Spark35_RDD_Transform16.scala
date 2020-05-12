package com.atguigu.bigdata.spark.core.operation.transform
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
object Spark35_RDD_Transform16 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)


        // TODO Scala - 转换算子 - K-V类型数据的操作
        val rdd = sc.makeRDD(List(1,2,3,4,5,6),2)

        // partitionBy : 按照指定的规则进行分区
        // RDD有些方法只能是特殊类型才能使用，而不是任何地方都能使用

        // partitionBy只能在K-V类型的RDD使用
        val rdd1: RDD[(Int, Int)] = rdd.map((_, 1))

        // Scala：隐式转换。
        // RDD => PairRDDFunctions

        // RDD支持分区器对K-V数据进行重分区
        // Spark默认采用的分区方式就是HashPartitioner
        // HashPartitioner的分区规则
        // val pindex = key.hashCode % numPartitions
        val rdd2: RDD[(Int, Int)] =
           rdd1.partitionBy(new HashPartitioner(3))

        // 如果多次重分区使用的分区器相同（类型，数量）
        // 不会再次重分区，因为没有意义。
        val rdd4 = rdd2.partitionBy(new HashPartitioner(3))

        val rdd3 = rdd4.mapPartitionsWithIndex(
            (index, data) => {
                data.map(
                    d => {
                        (index, d)
                    }
                )
            }
        )

        rdd3.collect().foreach(println)


        sc.stop
    }
}
