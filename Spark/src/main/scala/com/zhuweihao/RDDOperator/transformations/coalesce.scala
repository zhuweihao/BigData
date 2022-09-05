package com.zhuweihao.RDDOperator.transformations

import org.apache.spark.{SparkConf, SparkContext}

object coalesce {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4, 5, 6), 3)
    val newRDD = rdd.coalesce(2,true)
    println(newRDD.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 0) {
          iter
        } else {
          Nil.iterator
        }
      }
    ).collect().mkString(","))
    println(newRDD.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1) {
          iter
        } else {
          Nil.iterator
        }
      }
    ).collect().mkString(","))

    sparkContext.stop()
  }
}
