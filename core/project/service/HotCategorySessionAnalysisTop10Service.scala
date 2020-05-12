package com.atguigu.bigdata.spark.core.project.service

import com.atguigu.bigdata.spark.core.project.bean.{HotCategory, UserVisitAction}
import com.atguigu.bigdata.spark.core.project.common.TService
import com.atguigu.bigdata.spark.core.project.dao.{HotCategoryAnalysisTop10Dao, HotCategorySessionAnalysisTop10Dao}
import com.atguigu.bigdata.spark.core.project.helper.HotCategoryAccumulator
import com.atguigu.bigdata.spark.core.project.util.ProjectUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.util.control.Breaks

/**
  * 热门品类Top10业务对象
  */
class HotCategorySessionAnalysisTop10Service extends TService {
    private val hotCategorySessionAnalysisTop10Dao = new HotCategorySessionAnalysisTop10Dao

    def analysis1(categories:List[HotCategory]) = {

        // TODO 读取原始数据, 封装成样例类对象
        val fileRDD =
            hotCategorySessionAnalysisTop10Dao.readFile("input/user_visit_action.txt")

        val actionRDD =
            fileRDD.map(
                line => {
                    val datas = line.split("_")
                    UserVisitAction(
                        datas(0),
                        datas(1).toLong,
                        datas(2),
                        datas(3).toLong,
                        datas(4),
                        datas(5),
                        datas(6).toLong,
                        datas(7).toLong,
                        datas(8),
                        datas(9),
                        datas(10),
                        datas(11),
                        datas(12).toLong
                    )
                }
            )

        val cids = categories.map(_.id)
        // 使用广播变量提升性能
        val broadcastIds: Broadcast[List[String]] = ProjectUtil.sparkContext().broadcast(cids)

        println("action data count = " + actionRDD.count())
        // TODO 过滤数据，只保留Top10热门品类的数据, 点击行为的数据
        val filterRDD = actionRDD.filter(
            action => {
                // 点击行为保留
                if ( action.click_category_id != "-1" ) {
                    //cids.contains(action.click_category_id.toString)
                    broadcastIds.value.contains(action.click_category_id.toString)
                    // 判断点击的品类ID是否在Top10热门品类中
//                    var flg = false
//                    Breaks.breakable {
//                        for ( c <- categories ) {
//                            if ( c.id.toLong == action.click_category_id ) {
//                                flg = true
//                                Breaks.break()
//                            }
//                        }
//                    }
//                    flg
                } else {
                    false
                }
            }
        )
        println("filter action data count = " + filterRDD.count())
        // TODO 将数据转换成特定格式，用于统计 : （品类_session, 1）
        val categoryToSessionRDD = filterRDD.map(
            action => {
                ( action.click_category_id + "_" + action.session_id, 1 )
            }
        )

        // TODO 将转换后的数据进行分组聚合 : （品类_session, 1） => （品类_session, sum）
        val categoryToSessionSumRDD: RDD[(String, Int)] = categoryToSessionRDD.reduceByKey(_+_)

        // TODO 将聚合的结果进行结构的转换：（品类_session, sum）=> ( 品类，（ session, sum ） )
        val categoryToSessionSumRDD1 = categoryToSessionSumRDD.map{
            case ( k, sum ) => {
                val ks = k.split("_")
                ( ks(0), (ks(1), sum) )
            }
        }

        // TODO 将转换结构后的数据根据品类进行分组
        val groupRDD: RDD[(String, Iterable[(String, Int)])] = categoryToSessionSumRDD1.groupByKey()

        // TODO 将分组后的session数据进行排序，并取前10
        val mapValuesRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
            iter => {
                iter.toList.sortWith(
                    (left, right) => {
                        left._2 > right._2
                    }
                ).take(10)
            }
        )
        mapValuesRDD.collect()
    }
}
