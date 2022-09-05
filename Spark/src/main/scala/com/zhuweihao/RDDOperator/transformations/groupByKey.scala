package com.zhuweihao.RDDOperator.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object groupByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("b", 4),
    ))
    val newRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    newRDD.collect().foreach(println)
    val value: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)
    value.collect().foreach(println)

    sparkContext.stop()
  }
}
