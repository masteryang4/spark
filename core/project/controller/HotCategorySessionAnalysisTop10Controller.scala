package com.atguigu.bigdata.spark.core.project.controller

import com.atguigu.bigdata.spark.core.project.bean
import com.atguigu.bigdata.spark.core.project.common.TController
import com.atguigu.bigdata.spark.core.project.service.{HotCategoryAnalysisTop10Service, HotCategorySessionAnalysisTop10Service}

/**
  * 热门品类Top10控制器对象
  */
class HotCategorySessionAnalysisTop10Controller extends TController{
    private val hotCategoryAnalysisTop10Service = new HotCategoryAnalysisTop10Service
    private val hotCategorySessionAnalysisTop10Service = new HotCategorySessionAnalysisTop10Service
    override def execute(): Unit = {

        val categories: List[bean.HotCategory] = hotCategoryAnalysisTop10Service.analysis5()
        val result = hotCategorySessionAnalysisTop10Service.analysis1(categories)
        result.foreach(println)
    }
}
