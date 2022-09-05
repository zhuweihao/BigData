package com.zhuweihao.RDDOperator.actions

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object countByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("b", 4)
    ), 2)
    val value: collection.Map[(String, Int), Long] = rdd.countByValue()
    println(value)
    val value1: collection.Map[String, Long] = rdd.countByKey()
    println(value1)


    //foreach Driver端内存集合的循环遍历方法
    rdd.collect().foreach(println)
    //foreach Executor端内存数据打印
    rdd.foreach(println)


    sparkContext.stop()
  }
}
