package com.zhuweihao.RDDOperator.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object flatMap {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(
      List(1, 2), 3, List(4, 5)
    ))
    val flatRDD: RDD[Any] = rdd.flatMap(
      data => {
        data match {
          case list: List[_] => list
          case num => List(num)
        }
      }
    )
    flatRDD.foreach(println)

    sparkContext.stop()
  }
}
