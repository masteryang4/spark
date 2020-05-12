package com.atguigu.bigdata.spark.core.operation.transform
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark43_RDD_test {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)


        // TODO Scala - 转换算子
        val rdd =
            sc.makeRDD(
                List(
                    ("a", 88), ("b", 95), ("a", 91),
                    ("b", 93), ("a", 95), ("b", 98))
                ,2)

        // combineByKeyWithClassTag[V]((v: V) => v, func, func, partitioner)
        //rdd.reduceByKey()
        // combineByKeyWithClassTag[U]((v: V) => cleanedSeqOp(createZero(), v),
        //      cleanedSeqOp, combOp, partitioner)
        //rdd.aggregateByKey(0)(_+_, _+_)
        // combineByKeyWithClassTag[V]((v: V) => cleanedFunc(createZero(), v),
        //      cleanedFunc, cleanedFunc, partitioner)
        //rdd.foldByKey()
        // combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners,
        //      partitioner, mapSideCombine, serializer)(null)
        //rdd.combineByKey()

        sc.stop
    }
}
