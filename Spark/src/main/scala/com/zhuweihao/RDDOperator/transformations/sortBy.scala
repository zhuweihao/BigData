package com.zhuweihao.RDDOperator.transformations

import org.apache.spark.{SparkConf, SparkContext}

object sortBy {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    /*val rdd = sparkContext.makeRDD(List(5, 1, 2, 6, 3, 4))
    rdd.sortBy(num => num).collect().foreach(println)*/

    /*val rdd = sparkContext.makeRDD(List(("1", 1), ("11", 2), ("2", 3)), 2)
    rdd.sortBy(t=>t._1).collect().foreach(println)*/

    val rdd = sparkContext.makeRDD(List(("1", 1), ("11", 2), ("2", 3)), 2)
    rdd.sortBy(t=>t._1.toInt).collect().foreach(println)

    sparkContext.stop()
  }
}
