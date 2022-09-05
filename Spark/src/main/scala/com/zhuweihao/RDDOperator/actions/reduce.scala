package com.zhuweihao.RDDOperator.actions

import org.apache.spark.{SparkConf, SparkContext}

object reduce {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(1, 2, 4, 3),2)
    //分区一：1-2=-1，分区二：3-4=-1
    //分区间：-1-（-1）=0
    /*val i: Int = rdd.reduce(_ - _)
    println(i)*/

    /*val ints: Array[Int] = rdd.collect()
    println(ints.mkString(","))*/


    val l: Long = rdd.count()
    println(l)

    val i: Int = rdd.first()
    println(i)

    val ints: Array[Int] = rdd.take(3)
    println(ints.mkString(","))

    val ints1: Array[Int] = rdd.takeOrdered(3)
    println(ints1.mkString(","))

    val i1: Int = rdd.aggregate(1)(_ + _, _ + _)
    println(i1)
    println(rdd.fold(1)(_ + _))



    sparkContext.stop()
  }
}
