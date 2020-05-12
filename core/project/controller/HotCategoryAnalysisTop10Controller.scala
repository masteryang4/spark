package com.atguigu.bigdata.spark.core.project.controller

import com.atguigu.bigdata.spark.core.project.common.TController
import com.atguigu.bigdata.spark.core.project.service.HotCategoryAnalysisTop10Service

/**
  * 热门品类Top10控制器对象
  */
class HotCategoryAnalysisTop10Controller extends TController{
    private val hotCategoryAnalysisTop10Service = new HotCategoryAnalysisTop10Service
    override def execute(): Unit = {
        //val result = hotCategoryAnalysisTop10Service.analysis()
        //val result = hotCategoryAnalysisTop10Service.analysis2()
        //val result = hotCategoryAnalysisTop10Service.analysis3()
        val result = hotCategoryAnalysisTop10Service.analysis5()
        result.foreach(println)
    }
}
