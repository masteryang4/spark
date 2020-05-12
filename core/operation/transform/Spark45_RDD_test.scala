package com.atguigu.bigdata.spark.core.operation.transform
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark45_RDD_test {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)


        // TODO Scala - 转换算子 - sortByKey
        val rdd = sc.makeRDD(List(
            (new User(), 1), (new User(), 3), (new User(), 4), (new User(), 2)
        ))

        // sortByKey根据Key来进行排列，默认为升序
        val rdd1 = rdd.sortByKey(true)


        rdd1.collect().foreach(println)

        sc.stop
    }
    class User extends Ordered[User]{
        override def compare(that: User): Int = {
            1
        }
    }
}
