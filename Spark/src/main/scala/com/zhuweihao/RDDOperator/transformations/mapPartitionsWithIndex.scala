package com.zhuweihao.RDDOperator.transformations

import org.apache.spark.{SparkConf, SparkContext}

object mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4), 2)
    val mpirdd = rdd.mapPartitionsWithIndex(
      (index, iterator) => {
        if (index == 1) {
          iterator
        } else {
          //Nil为空集合
          Nil.iterator
        }
      }
    )
    mpirdd.collect().foreach(println)

    sparkContext.stop()
  }
}
