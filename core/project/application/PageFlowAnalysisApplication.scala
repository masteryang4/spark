package com.atguigu.bigdata.spark.core.project.application

import com.atguigu.bigdata.spark.core.project.common.TApplication
import com.atguigu.bigdata.spark.core.project.controller.{HotCategoryAnalysisTop10Controller, PageFlowAnalysisController}

/**
  * 页面单跳转换率
  */
object PageFlowAnalysisApplication extends App with TApplication{

    start( appName = "PageFlowAnalysis" ) {
        val controller = new PageFlowAnalysisController
        controller.execute()
    }

}
