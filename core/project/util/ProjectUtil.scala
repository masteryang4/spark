package com.atguigu.bigdata.spark.core.project.util

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 项目工具类
  */
object ProjectUtil {

    private val scLocal : ThreadLocal[SparkContext] = new ThreadLocal[SparkContext]

    def sparkContext(master:String = "local[*]", appName:String = "application") = {
        // 从当前线程的内存中获取数据
        var sc = scLocal.get()
        if ( sc == null ) {
            val conf = new SparkConf().setAppName(appName).setMaster(master)
            sc = new SparkContext(conf)
            // 将数据存储到当前线程的内存中
            scLocal.set(sc)
        }
        sc
    }

    def stop(): Unit = {
        var sc = scLocal.get()
        if ( sc != null ) {
            sc.stop()
        }
        scLocal.remove()
    }
}
