package com.atguigu.bigdata.spark.core.operation.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Action6 {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        // TODO Spark - RDD - 序列化
        val rdd = sc.makeRDD(
            List(

            )
        )
        // TODO : java.io.NotSerializableException: com.atguigu.bigdata.spark.action.Spark07_RDD_Action6$User
        // 在Driver端创建了内存中的数据对象，打印时是发送到Executor端打印
        // 所以需要这个数据对象可以在网络中传递，那么就必须可序列化
        // Executor接收到数据后应该能够反序列化。
        val user = new User()

        // TODO : Task not serializable
        // java.io.NotSerializableException: com.atguigu.bigdata.spark.action.Spark07_RDD_Action6$User

        // 闭包
        // Spark中的算子基本上都会传递匿名函数
        // 而匿名函数都是有闭包的。有可能引用外部的变量,这个时候，外部的变量在执行的时候
        // 会发送到Executor端执行。那么如果引用变量不能序列化，就会发生错误。
        // 所以在执行前，spark必须要进行检测，看看引用的变量是否能够序列化
        // 这个操作称之为：闭包检测功能
        rdd.foreach(
            data => {
                //println("*************")
                println(user)
            }
        )

        sc.stop

    }
    class User {

    }
    class Emp extends Serializable {

    }
}
