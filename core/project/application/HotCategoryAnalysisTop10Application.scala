package com.atguigu.bigdata.spark.core.project.application

import com.atguigu.bigdata.spark.core.project.common.TApplication
import com.atguigu.bigdata.spark.core.project.controller.HotCategoryAnalysisTop10Controller

/**
  * 热门品类Top10应用
  */
object HotCategoryAnalysisTop10Application extends App with TApplication{

    start( appName = "HotCategoryAnalysisTop10" ) {
        val controller = new HotCategoryAnalysisTop10Controller
        controller.execute()
    }

}
