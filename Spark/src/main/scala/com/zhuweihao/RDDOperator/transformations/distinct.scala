package com.zhuweihao.RDDOperator.transformations

import org.apache.spark.{SparkConf, SparkContext}

object distinct {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4))
    val distinctRDD = rdd.distinct()
    distinctRDD.collect().foreach(println)


    sparkContext.stop()
  }
}
