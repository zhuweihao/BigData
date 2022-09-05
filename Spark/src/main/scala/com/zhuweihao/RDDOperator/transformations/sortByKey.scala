package com.zhuweihao.RDDOperator.transformations

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object sortByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(
      ("a", 1), ("f", 2), ("c", 3), ("e", 8)
    ), 2)
    val value: RDD[(String, Int)] = rdd.sortByKey(false)
    value.collect().foreach(println)


    sparkContext.stop()
  }
}
