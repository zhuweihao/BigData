package com.zhuweihao.RDDOperator.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object map1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    //TODO 算子-map 实现提取用户请求URL资源路径
    val rdd = sparkContext.textFile("Spark/src/main/resources/apache.log")
    val mapRDD: RDD[String] = rdd.map(
      line => {
        val datas = line.split(" ")
        datas(6)
      }
    )
    mapRDD.collect().foreach(println)

    sparkContext.stop()
  }
}
