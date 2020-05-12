package com.atguigu.bigdata.spark.core.project.application

import com.atguigu.bigdata.spark.core.project.common.TApplication
import com.atguigu.bigdata.spark.core.project.controller.WordCountController
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object WordCountApplication extends App with TApplication{

    // 启动应用
    // 带名参数
    start(appName="WordCount"){
        val controller = new WordCountController
        // 执行控制器
        controller.execute()
    }

}
