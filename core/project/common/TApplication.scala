package com.atguigu.bigdata.spark.core.project.common

import com.atguigu.bigdata.spark.core.project.util.ProjectUtil
import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {

    /*
       模板方法设计模式
       1. 在父类中搭建算法的骨架，但是没有具体的实现
       2. 在每一个子类中重写方法，完成自己功能的实现

       // 控制抽象：可以将代码逻辑作为参数进行传递
       // 函数柯里化:
       // 参数默认值
     */
    def start(master:String = "local[*]", appName:String = "application")(op: => Unit): Unit = {
        // 创建Spark的配置对象
        ProjectUtil.sparkContext(master, appName)

        // 执行业务逻辑
        try {
            op
        } catch {
            case ex:Exception => ex.printStackTrace
        }

        // 关闭连接对象
        ProjectUtil.stop()
    }
}
