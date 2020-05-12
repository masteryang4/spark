package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object SparkSQL03_RDD_DataFrame_DataSet {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val rdd = spark.sparkContext.makeRDD(List(
            ("zhangsan", 30), ("wangwu", 40), ("lisi", 50)
        ))

        // TODO RDD => DataFrame
        //val df: DataFrame = rdd.toDF("name", "age")

        // TODO DataFrame => DataSet
        // val ds: Dataset[Person] = df.as[Person]

        //ds.show()

        // TODO RDD => DataSet
        // 使用样例类将RDD转换为Dataset
        val personRDD = rdd.map{
            case (name, age) => {
                Person(name, age)
            }
        }

        val ds: Dataset[Person] = personRDD.toDS()

        // TODO DataSet => DataFrame
        // 进行转换时，DataFrame不关心类型，所以返回的类型为Row类型
        val df: DataFrame = ds.toDF()
//        df.foreach(
//            row => {
//                println(row(0))
//            }
//        )

        // TODO DataFrame => RDD
        val rdd1: RDD[Row] = df.rdd

        // TODO Dataset => RDD
        val rdd2: RDD[Person] = ds.rdd

        spark.close

    }
    case class Person(name:String, age:Int)
}
