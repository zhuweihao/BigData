package com.zhuweihao

import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.GraphLoader

import java.util.logging.{Level, Logger}

/**
 * @Author zhuweihao
 * @Date 2022/8/2 16:46
 * @Description com.zhuweihao
 */
object PageRankExample {
  val FOLLOWERS_PATH = "Spark/src/main/resources/followers.txt"
  val USERS_PATH = "Spark/src/main/resources/users.txt"

  def main(args: Array[String]): Unit = {
    // 关闭 Spark 内部的日志打印，只关注结果日志
    Logger.getLogger("org").setLevel(Level.OFF)
    // 创建 SparkSession
    val spark = SparkSession
      .builder
      .appName("PageRankExample")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    // 加载边作为图
    val graph = GraphLoader.edgeListFile(sc, FOLLOWERS_PATH)
    // 运行 PageRank
    val ranks = graph.pageRank(0.0001).vertices
    // Join ranks with the usernames
    /*val users = sc.textFile(USERS_PATH).map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    // 打印结果
    println(ranksByUsername.collect().mkString("\n"))*/

    Thread.sleep(500000)
    spark.stop()
  }
}
