package com.atguigu.bigdata.spark.core.project.common

import com.atguigu.bigdata.spark.core.project.util.ProjectUtil

trait TDao {

    def readFile( path:String ) = {
        ProjectUtil.sparkContext().textFile(path)
    }
}
