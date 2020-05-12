package com.atguigu.bigdata.spark.core.project.helper

import com.atguigu.bigdata.spark.core.project.bean.HotCategory
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * 热门品类累加器
  * (String, String) : 输入的数据(品类，行为类型)
  * Map[String, HotCategory] ： 表示品类对应的点击数量
  */
class HotCategoryAccumulator extends AccumulatorV2[(String,String), mutable.Map[String, HotCategory]] {
    private var hotCategoryMap = mutable.Map[String, HotCategory]()

    /**
      * 判断累加器是否初始化
      * @return
      */
    override def isZero: Boolean = hotCategoryMap.isEmpty

    /**
      * 累加器的复制
      * @return
      */
    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
        new HotCategoryAccumulator
    }

    /**
      * 重置累加器
      */
    override def reset(): Unit = {
        hotCategoryMap.clear()
    }

    /**
      * 增加数据
      * @param v
      */
    override def add(v: (String, String)): Unit = {
        val categoryId = v._1
        val actionType = v._2

        val hc = hotCategoryMap.getOrElse(categoryId, HotCategory(categoryId,0,0,0))

        actionType match {
            case "click" => hc.clickCount += 1
            case "order" => hc.orderCount += 1
            case "pay" => hc.payCount += 1
            case _ =>
        }

        //hotCategoryMap(categoryId) = hc
        hotCategoryMap.update(categoryId, hc)
    }

    /**
      * 合并累加器的值
      * @param other
      */
    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {

        /*
            var map1 = this.innerMap
            var map2 = other.value

            innerMap = map1.foldLeft(map2)(
                (map, kv) => {
                    val k = kv._1
                    val v = kv._2
                    map(k) = map.getOrElse(k, 0) + v
                    map
                }
            )
         */

        other.value.foreach{
            case ( cid, hotCategory ) => {
                val hc = hotCategoryMap.getOrElse(cid, HotCategory(cid,0,0,0))
                hc.clickCount += hotCategory.clickCount
                hc.orderCount += hotCategory.orderCount
                hc.payCount += hotCategory.payCount

                // Java中可以不需要下面这行代码
                hotCategoryMap(cid) = hc
            }
        }

    }

    /**
      * 获取累加器的值
      * @return
      */
    override def value: mutable.Map[String, HotCategory] = hotCategoryMap
}
