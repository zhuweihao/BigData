package com.zhuweihao.RDDOperator.transformations

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object join {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3), ("e", 8)
    ), 2)
    val rdd1 = sparkContext.makeRDD(List(
      ("a", "4"), ("b", "5"), ("c", 6), ("d", 7), ("a", 3)
    ), 2)
    val newRDD: RDD[(String, (Int, Any))] = rdd.join(rdd1)
    newRDD.collect().foreach(println)


    sparkContext.stop()
  }
}
