package com.zhuweihao.RDDOperator.transformations

import org.apache.spark.{SparkConf, SparkContext}

object filter {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    /*val rdd = sparkContext.makeRDD(List(1, 2, 3, 4))
    val filterRDD = rdd.filter(num => num % 2 != 0)
    filterRDD.collect().foreach(println)*/
    val rdd = sparkContext.textFile("Spark/src/main/resources/apache.log")

    rdd.filter(
      line => {
        val strings = line.split(" ")
        strings(3).startsWith("17/05/2015")
      }
    ).map(
      line => {
        val datas = line.split(" ")
        datas(6)
      }
    ).collect().foreach(println)


    sparkContext.stop()
  }
}
