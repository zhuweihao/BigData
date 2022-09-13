package com.zhuweihao.Case

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author zhuweihao
 * @Date 2022/7/31 22:28
 * @Description com.zhuweihao
 */
object pipline {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("SparkPipeline")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(Array(1, 2, 3, 4))
    //执行filter
    val filterRdd = rdd.filter {
      x => {
        println("fliter********" + x)
        true
      }
    }
    //执行map
    filterRdd.map {
      x => {
        println("map--------" + x)
        x
      }
    }.count() //触发执行
    sc.stop()


  }
}
