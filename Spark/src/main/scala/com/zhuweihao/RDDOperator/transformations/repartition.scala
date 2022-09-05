package com.zhuweihao.RDDOperator.transformations

import org.apache.spark.{SparkConf, SparkContext}

object repartition {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val newRDD = rdd.repartition(3)
    for (i <- 0 to 2) {
      println(newRDD.mapPartitionsWithIndex(
        (index, iter) => {
          if (index == i) {
            iter
          } else {
            Nil.iterator
          }
        }
      ).collect().mkString(","))
    }


    sparkContext.stop()
  }
}
