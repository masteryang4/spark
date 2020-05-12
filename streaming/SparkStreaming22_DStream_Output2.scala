package com.atguigu.bigdata.spark.streaming

import java.sql.{DriverManager, PreparedStatement}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming22_DStream_Output2 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
        val ssc = new StreamingContext(sparkConf, Seconds(5))

        val socketDS = ssc.socketTextStream("localhost", 9999)

        // 将数据保存到MySQL数据库中
        // id, name, age



        socketDS.foreachRDD(rdd=>{
            // 以分区为单位进行转换 => 返回
            //rdd.mapPartitions()
            // 以分区为单位进行遍历 => 不需要返回
            rdd.foreachPartition(
                datas => {
                    // TODO 加载数据库驱动
                    Class.forName("com.mysql.jdbc.Driver")
                    // TODO 建立链接和操作对象
                    // TODO 所有的连接对象都不支持序列化操作
                    val conn =
                    DriverManager.getConnection(
                        "jdbc:mysql://linux1:3306/rdd",
                        "root","000000")
                    val sql = "insert into user (id ,name, age) values (?, ?, ?)"
                    val statement: PreparedStatement = conn.prepareStatement(sql)

                    // datas 其实是scala的集合，所以不存在分布式计算的概念
                    datas.foreach(
                        data => {
                            // 解决性能问题
                            val datas = data.split(",")
                            val id = datas(0).toInt
                            val name = datas(1)
                            val age = datas(2).toInt

                            statement.setInt(1, id)
                            statement.setString(2, name)
                            statement.setInt(3, age)
                            // TODO 操作数据
                            //statement.addBatch()
                            //statement.executeBatch()
                            statement.executeUpdate()

                            println("数据保存成功！！！")
                        }
                    )

                    // TODO 关闭连接
                    statement.close()
                    conn.close()
                }
            )
        })


        ssc.start()
        ssc.awaitTermination()

    }
}
