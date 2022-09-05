package com.zhuweihao.RDDOperator.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object aggregateByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    /*val rdd = sparkContext.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("a", 4)
    ), 2)
    //函数编程中，接受多个参数的函数都可以转化为接受单个参数的函数，这个转化过程就叫柯里化
    /**
     * aggregateByKey存在函数柯里化，有两个参数列表
     * 第一个参数列表，需要传递一个参数，表示为初始值，主要用于当碰见第一个key的时候，和value进行分区内计算
     * 第二个参数列表，第一个参数表示分区内计算规则，第二个参数表示分区间计算规则
     */
    val value: RDD[(String, Int)] = rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    )
    value.collect().foreach(println)

    rdd.foldByKey(0)(
      (x, y) => x + y
    )*/

    val rdd = sparkContext.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)
    ), 2)
    //获取相同key的数据的平均值
    val newRDD: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      (tuple, value) => {
        (tuple._1 + value, tuple._2 + 1)
      },
      (tuple1, tuple2) => {
        (tuple1._1 + tuple2._1, tuple1._2 + tuple2._2)
      }
    )
    val resultRDD: RDD[(String, Int)] = newRDD.mapValues {
      case (num, cnt) => {
        num / cnt
      }
    }
    resultRDD.collect().foreach(println)


    sparkContext.stop()
  }
}
