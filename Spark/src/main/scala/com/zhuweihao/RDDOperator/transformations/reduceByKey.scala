package com.zhuweihao.RDDOperator.transformations

import org.apache.spark.{SparkConf, SparkContext}

object reduceByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("b", 4),
    ))
    //reduceByKey:相同的key的数据进行value数据的聚合操作
    //scala语言中一般的聚合操作都是两两聚合，spark基于scala开发，所以他的聚合也是两两聚合
    //[1,2,3]=>[3,3]=>[6]
    val reduceRDD = rdd.reduceByKey((x: Int, y: Int) => {
      println(s"x=${x},y=${y}")
      x + y
    })
    reduceRDD.collect().foreach(println)


    sparkContext.stop()
  }
}
