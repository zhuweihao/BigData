package com.zhuweihao.RDDOperator.transformations

import org.apache.spark.{SparkConf, SparkContext}

object map {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    //rdd在计算一个分区内数据时是串行有序的
    //不同分区数据计算是无序的
    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4), 2)
    val mapRDD = rdd.map(
      num => {
        println("--------" + num)
        num
      }
    )
    val mapRDD1 = mapRDD.map(
      num => {
        println("+++++++++" + num)
        num
      }
    )
    mapRDD1.collect()

    sparkContext.stop()
  }
}
