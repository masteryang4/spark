package com.atguigu.bigdata.spark.core.project.service

import com.atguigu.bigdata.spark.core.project.bean.{HotCategory, UserVisitAction}
import com.atguigu.bigdata.spark.core.project.common.TService
import com.atguigu.bigdata.spark.core.project.dao.HotCategoryAnalysisTop10Dao
import com.atguigu.bigdata.spark.core.project.helper.HotCategoryAccumulator
import com.atguigu.bigdata.spark.core.project.util.ProjectUtil
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * 热门品类Top10业务对象
  */
class HotCategoryAnalysisTop10Service extends TService {
    private val hotCategoryAnalysisTop10Dao = new HotCategoryAnalysisTop10Dao

    /**
      * 数据分析5
      *
      * 不使用shuffle也可以聚合数据,使用累加器实现数据聚合
      */
    def analysis5() = {
        val dataRDD: RDD[String] = hotCategoryAnalysisTop10Dao.readFile("input/user_visit_action.txt")

        // （xxx, zs, 鞋，click）
        // （xxx, zs, 鞋，order）

        //  HotCategory(鞋, 11 , 18, 12)
        //  HotCategory(衣服, 11 , 18, 12)

        // TODO 创建累加器
        val acc = new HotCategoryAccumulator

        // TODO 注册累加器
        ProjectUtil.sparkContext().register(acc, "HotCategoryAccumulator")

        // TODO 使用累加器
        dataRDD.foreach(
            data => {
                val datas = data.split("_")
                if ( datas(6) != "-1" ) {
                    // 点击的场合
                    acc.add((datas(6), "click"))
                } else if ( datas(8) != "null" ) {
                    // 下单的场合
                    val ids: Array[String] = datas(8).split(",")
                    ids.foreach(
                        id => {
                            acc.add((id, "order"))
                        }
                    )
                } else if ( datas(10) != "null" ) {
                    // 支付的场合
                    val ids: Array[String] = datas(10).split(",")
                    ids.foreach(
                        id => {
                            acc.add((id, "pay"))
                        }
                    )
                }

            }
        )

        // TODO 获取累加器的结果
        val accMap: mutable.Map[String, HotCategory] = acc.value
        val categories: mutable.Iterable[HotCategory] = accMap.map(_._2)

        // TODO 排序后取前10名
        categories.toList.sortWith(
            (left, right) => {
                if ( left.clickCount > right.clickCount ) {
                    true
                } else if (left.clickCount == right.clickCount) {
                    if ( left.orderCount > right.orderCount ) {
                        true
                    } else if ( left.orderCount == right.orderCount ) {
                        left.payCount > right.payCount
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
        ).take(10)
    }

    /**
      * 数据分析4
      */
    def analysis4() = {
        val dataRDD: RDD[String] = hotCategoryAnalysisTop10Dao.readFile("input/user_visit_action.txt")
        val categoryRDD = dataRDD.flatMap(
            data => {
                val datas = data.split("_")
                if (datas(6) != "-1") {
                    // 点击的场合
                    List((datas(6), HotCategory(datas(6), 1, 0, 0)))
                } else if (datas(8) != "null") {
                    // 下单的场合
                    val ids = datas(8).split(",")
                    ids.map(
                        id => {
                            (id, HotCategory(id, 0, 1, 0))
                        }
                    )
                } else if (datas(10) != "null") {
                    // 支付的场合
                    val ids = datas(10).split(",")
                    ids.map(
                        id => {
                            (id, HotCategory(id, 0, 0, 1))
                        }
                    )
                } else {
                    Nil
                }
            }
        )

        val reduceRDD: RDD[HotCategory] = categoryRDD.reduceByKey(
            (c1, c2) => {
                c1.clickCount = c1.clickCount + c2.clickCount
                c1.orderCount = c1.clickCount + c2.orderCount
                c1.payCount = c1.payCount + c2.payCount
                c1
            }
        ).map(_._2)

        reduceRDD.sortBy(
            data => {
                (data.clickCount, data.orderCount, data.payCount)
            },
            false
        ).take(10)
    }

    /**
      * 数据分析3
      *
      * 将数据封装为样例类，操作方便
      */
    def analysis3() = {
        val dataRDD: RDD[String] = hotCategoryAnalysisTop10Dao.readFile("input/user_visit_action.txt")
        val categoryRDD = dataRDD.flatMap(
            data => {
                val datas = data.split("_")
                if (datas(6) != "-1") {
                    // 点击的场合
                    List(HotCategory(datas(6), 1, 0, 0))
                } else if (datas(8) != "null") {
                    // 下单的场合
                    val ids = datas(8).split(",")
                    ids.map(
                        id => {
                            HotCategory(id, 0, 1, 0)
                        }
                    )
                } else if (datas(10) != "null") {
                    // 支付的场合
                    val ids = datas(10).split(",")
                    ids.map(
                        id => {
                            HotCategory(id, 0, 0, 1)
                        }
                    )
                } else {
                    Nil
                }
            }
        )

        val groupRDD: RDD[(String, Iterable[HotCategory])] = categoryRDD.groupBy(_.id)

        val categoryMapRDD: RDD[HotCategory] = groupRDD.mapValues(
            iter => {
                iter.reduce(
                    (c1, c2) => {
                        c1.clickCount = c1.clickCount + c2.clickCount
                        c1.orderCount = c1.clickCount + c2.orderCount
                        c1.payCount = c1.payCount + c2.payCount
                        c1
                    }
                )
            }
        ).map(_._2)

        categoryMapRDD.collect().sortWith(
            (left, right) => {
                if ( left.clickCount > right.clickCount ) {
                    true
                } else if ( left.clickCount == right.clickCount ) {
                    if ( left.orderCount > right.orderCount ) {
                        true
                    } else if ( left.orderCount == right.orderCount ) {
                        left.payCount > right.payCount
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
        ).take(10)
    }

    /**
      * 数据分析2
       // （xxx, zs, 鞋，click）
      // （鞋，click, 1）=> (鞋，click, 1, order,0, pay, 0)
      // （鞋，click, 1）=> (鞋，click, 1, order,0, pay, 0)
      // （鞋，click, 1）=> (鞋，click, 1, order,0, pay, 0)

      // (鞋，click, 11) => (鞋，click, 11, order,0, pay, 0)
      // (鞋，order, 12) => (鞋，click, 0, order,12, pay, 0)
      // (鞋，pay, 17)   => (鞋，click, 0, order,0, pay, 17)

      // （鞋， 11， 12， 17）
      */
    def analysis2() = {

        val dataRDD: RDD[String] = hotCategoryAnalysisTop10Dao.readFile("input/user_visit_action.txt")

        val categoryRDD: RDD[(String, (Int, Int, Int))] = dataRDD.flatMap(
            data => {
                val datas = data.split("_")
                if (datas(6) != "-1") {
                    // 点击的场合
                    List((datas(6), (1, 0, 0)))
                } else if (datas(8) != "null") {
                    // 下单的场合
                    val ids = datas(8).split(",")
                    ids.map((_, (0, 1, 0)))
                } else if (datas(10) != "null") {
                    // 支付的场合
                    val ids = datas(10).split(",")
                    ids.map((_, (0, 0, 1)))
                } else {
                    Nil
                }
            }
        )
        val categorySumRDD: RDD[(String, (Int, Int, Int))] = categoryRDD.reduceByKey(
            (c1, c2) => {
                (c1._1 + c2._1, c1._2 + c2._2, c1._3 + c2._3)
            }
        )
        categorySumRDD.sortBy(_._2, false).take(10)
    }
    /**
      * 数据分析1
      */
    override def analysis() = {

        // TODO 获取数据
        val dataRDD: RDD[String] = hotCategoryAnalysisTop10Dao.readFile("input/user_visit_action.txt")
        // 因为RDD需要重复使用，所以可以将RDD缓存起来，重复使用。
        val dataCacheRDD: RDD[String] = dataRDD.cache()

        // TODO 将原始数据进行转换，形成对象，方便使用
        // TODO 品类点击统计
        val categoryClickRDD: RDD[(String, Int)] = dataCacheRDD.map(
            data => {
                val datas = data.split("_")
                // （品类，点击总数）
                (datas(6), 1)
            }
        )
        // (品类， 1)
        val categoryClickFilterRDD = categoryClickRDD.filter(_._1 != "-1")
        val categoryClickReduceRDD: RDD[(String, Int)] = categoryClickFilterRDD.reduceByKey(_+_)

        // TODO 品类下单统计
        val categoryOrderRDD: RDD[String] = dataCacheRDD.map(
            data => {
                val datas = data.split("_")
                // （品类，下单总数）
                datas(8)
            }
        )

        // 1,2,3,4
        val categoryOrderFilterRDD = categoryOrderRDD.filter(_ != "null")

        // 1 - 2 - 3 - 4
        // (1,1),(2,1),(3,1)(4,1)
        val categoryOrdersRDD: RDD[(String, Int)] = categoryOrderFilterRDD.flatMap(
            data => {
                val ids = data.split(",")
                ids.map((_, 1))
            }
        )
        //（品类，下单总数）
        val categoryOrderReduceRDD: RDD[(String, Int)] = categoryOrdersRDD.reduceByKey(_+_)

        // TODO 品类支付统计
        val categoryPayRDD: RDD[String] = dataCacheRDD.map(
            data => {
                val datas = data.split("_")
                // （品类，支付总数）
                datas(10)
            }
        )

        // 1,2,3,4
        val categoryPayFilterRDD = categoryPayRDD.filter(_ != "null")

        // 1 - 2 - 3 - 4
        // (1,1),(2,1),(3,1)(4,1)
        val categoryPaysRDD: RDD[(String, Int)] = categoryPayFilterRDD.flatMap(
            data => {
                val ids = data.split(",")
                ids.map((_, 1))
            }
        )
        //（品类，支付总数）
        val categoryPayReduceRDD: RDD[(String, Int)] = categoryPaysRDD.reduceByKey(_+_)

        // TODO 将之前分别的获取的RDD进行合并

        // (鞋，click, 11) => (鞋，click, 11, order,0, pay, 0)
        // (鞋，order, 12) => (鞋，click, 0, order,12, pay, 0)
        // (鞋，pay, 17)   => (鞋，click, 0, order,0, pay, 17)
        // => reduce
        // (鞋， click, 11, order,12, pay, 17)
        // reduce (A1, A1) => A1

        // TODO 将数据进行格式的转换
        // (category, count) => (category, clickcount, ordercount, paycount)
        val clickRDD =
            categoryClickReduceRDD.map{
                case (category, clickcount) => {
                    ( category, ( clickcount, 0, 0 ) )
                }
            }
        val orderRDD =
            categoryOrderReduceRDD.map {
                case (category, ordercount) => {
                    ( category, ( 0, ordercount, 0 ) )
                }
            }
        val payRDD =
            categoryPayReduceRDD.map {
                case (category, paycount) => {
                    ( category, ( 0, 0, paycount ) )
                }
            }

        val categoryRDD: RDD[(String, (Int, Int, Int))] =
            clickRDD.union(orderRDD).union(payRDD)

        // TODO 将合并的数据进行分组聚合
        // (鞋， (1, 12, 17))
        val categorySumRDD: RDD[(String, (Int, Int, Int))] = categoryRDD.reduceByKey(
            (c1, c2) => {
                (c1._1 + c2._1, c1._2 + c2._2, c1._3 + c2._3)
            }
        )

        // TODO 对分组聚合后的数据进行排序(降序)，并取前10个
        val tuples: Array[(String, (Int, Int, Int))] = categorySumRDD.sortBy(_._2, false).take(10)

        tuples
    }
}
