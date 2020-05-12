package com.atguigu.bigdata.spark.core.project.dao

import com.atguigu.bigdata.spark.core.project.common.TDao
import com.atguigu.bigdata.spark.core.project.util.ProjectUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

class WordCountDao extends TDao{

    def readData() : RDD[String] = {
        ProjectUtil.sparkContext().makeRDD(List("Hello", "Scala"))
    }
}
