package com.zhuweihao.RDDOperator.transformations

import org.apache.spark.{SparkConf, SparkContext}

object mapPartitions {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4))
    val mapPartitionsRDD = rdd.mapPartitions(
      iterator => {
        List(iterator.max).iterator
      }
    )
    mapPartitionsRDD.collect().foreach(println)

    sparkContext.stop()
  }
}
