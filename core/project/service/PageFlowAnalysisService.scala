package com.atguigu.bigdata.spark.core.project.service

import com.atguigu.bigdata.spark.core.project.bean.{HotCategory, UserVisitAction}
import com.atguigu.bigdata.spark.core.project.common.TService
import com.atguigu.bigdata.spark.core.project.dao.{HotCategoryAnalysisTop10Dao, PageFlowAnalysisDao}
import com.atguigu.bigdata.spark.core.project.helper.HotCategoryAccumulator
import com.atguigu.bigdata.spark.core.project.util.ProjectUtil
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * 页面单跳转换率
  */
class PageFlowAnalysisService extends TService {
    private val pageFlowAnalysisDao = new PageFlowAnalysisDao

    override def analysis() = {

        // TODO 读取原始数据，封装样例类
        val fileRDD =
            pageFlowAnalysisDao.readFile("input/user_visit_action.txt")

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
        val actionCacheRDD = actionRDD.cache()

        // TODO 对指定的页面跳转进行转换率的统计
        // 1-2 / 1, 2-3 / 2, 3-4 /3, 4-5/4, 5-6 / 5, 6-7/6
        val flowids = List(1,2,3,4,5,6,7)
        val zips: List[(Int, Int)] = flowids.zip(flowids.tail)
        val zipsString = zips.map{
            case (id1, id2) => {
                id1 + "-" + id2
            }
        }

        // TODO 计算分母
        // TODO 将数据进行结构的转换 (pageid, 1)
        val filterRDD = actionCacheRDD.filter(
            action => {
                flowids.contains(action.page_id.toInt)
            }
        )
        val pageToOneRDD = filterRDD.map(
               action => {
                   ( action.page_id, 1 )
               }
        )
        // TODO 将转换后的数据进行分组聚合 (pageid, 1) => (pageid, sum)
        val pageToSumRDD = pageToOneRDD.reduceByKey(_+_)
        val pageCount: Array[(Long, Int)] = pageToSumRDD.collect
        val pageCountMap = pageCount.toMap

        // TODO 计算分子
        // TODO 将数据根据session进行分组
        val groupRDD: RDD[(String, Iterable[UserVisitAction])] = actionCacheRDD.groupBy(_.session_id)

        // TODO 将分组后的数据进行排序（升序）
        val rdd: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
            iter => {
                val sortAction: List[UserVisitAction] = iter.toList.sortWith(
                    (left, right) => {
                        left.action_time < right.action_time
                    }
                )
                // TODO 将多个页面组合成连续跳转的页面数据
                // pageid1 , pageid2, pageid3
                // pageid1-pageid2, pageid2-pageid3
                // 1-2, 2-3 => (1-2,1), (2-3,1)
                // List(1,2,3)
                val ids: List[Long] = sortAction.map(_.page_id)
                // zip
                // 1, 2, 3
                // 2, 3
                val zipIds: List[(Long, Long)] = ids.zip(ids.tail)
                val zipIdToOne = zipIds.map {
                    case (id1, id2) => {
                        (id1 + "-" + id2, 1)
                    }
                }
                // (1-2,1)(2-3,1)(3-9,1)
                zipIdToOne.filter(
                    t => {
                        zipsString.contains(t._1)
                    }
                )
            }
        )
        val idToOneList: RDD[List[(String, Int)]] = rdd.map(_._2)
        val idToOneRDD: RDD[(String, Int)] = idToOneList.flatMap(list=>list)

        // TODO 将组合后的数据进行分组聚合
        // （pageid1-pageid2， 1） => (pageid1-pageid2, sum)
        val idToSumRDD: RDD[(String, Int)] = idToOneRDD.reduceByKey(_+_)

        // TODO 计算单跳转换率
        // 分子 / 分母
        idToSumRDD.foreach{
            case ( pageids, count ) => {
                val ids = pageids.split("-")
                // 查找分母
                // 1-2 => 2/1
                val count1 = pageCountMap(ids(0).toLong)
                println(pageids + "转换率为" + ( count.toDouble / count1 ))
            }
        }
    }
}
