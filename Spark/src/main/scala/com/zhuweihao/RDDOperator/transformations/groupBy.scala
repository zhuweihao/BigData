package com.zhuweihao.RDDOperator.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat

object groupBy {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    /*val rdd = sparkContext.makeRDD(List(1, 2, 3, 4), 2)
    val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(
      num => {
        println("-----------")
        num % 2
      }
    )
    groupRDD.collect().foreach(println)*/

    /*val rdd1 = sparkContext.makeRDD(List("Hello", "World", "Spark", "Hadoop"), 2)
    val groupRDD1 = rdd1.groupBy(_.charAt(0))
    val groupRDD2 = groupRDD1.mapPartitionsWithIndex(
      (index, iterator) => {
        iterator.map(
          data => {
            (index, data)
          }
        )
      }
    )
    groupRDD2.collect().foreach(println)*/

    val rdd = sparkContext.textFile("Spark/src/main/resources/apache.log")

    val timeRDD = rdd.map(
      line => {
        val datas = line.split(" ")
        val time = datas(3)
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val date = simpleDateFormat.parse(time)
        val simpleDateFormat1 = new SimpleDateFormat("dd/MM/yyyy:HH")
        val hour = simpleDateFormat1.format(date)
        (hour, 1)
      }
    ).groupBy(_._1)
    timeRDD.map {
      case (hour, iter) => {
        (hour, iter.size)
      }
    }.collect.foreach(println)

    sparkContext.stop()
  }
}
