package com.zhuweihao.RDDOperator.transformations

import org.apache.spark.{SparkConf, SparkContext}

object intersection {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    val rdd1 = sparkContext.makeRDD(List(1, 2, 3, 4),4)
    val rdd2 = sparkContext.makeRDD(List(1, 2, 5, 6),4)

    println(rdd1.intersection(rdd2).collect().mkString(","))
    println(rdd1.union(rdd2).collect().mkString(","))
    println(rdd1.subtract(rdd2).collect().mkString(","))
    println(rdd1.zip(rdd2).collect().mkString(","))
    println(rdd1.cartesian(rdd2).collect().mkString(","))


    sparkContext.stop()
  }
}
