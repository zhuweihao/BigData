package com.zhuweihao

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @Author zhuweihao
 * @Date 2022/8/1 1:12
 * @Description com.zhuweihao
 */
object PageRank {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("pagerank")
    val sparkContext = new SparkContext(sparkConf)
    val alpha = 0.85
    val iterCnt = 3
    val links: RDD[(String, List[String])] = sparkContext.parallelize(
      List(
        ("A", List("B", "C", "D")),
        ("B", List("A", "D")),
        ("C", List("C")),
        ("D", List("B", "C")))
    )
      .partitionBy(new HashPartitioner(2))
      .persist()

    var ranks: RDD[(String, Double)] = links.mapValues(_ => 0.25)

    for (i <- 0 until iterCnt) {
      val contributions: RDD[(String, Double)] = links.join(ranks).values.flatMap {
        case (linkList, rank) =>
          linkList.map(link => (link, rank / linkList.size))
      }
      ranks = contributions.reduceByKey((x, y) => x + y)
        .mapValues(v => {
          (1 - alpha) + alpha * v
        })
    }

    val output: Array[(String, Double)] = ranks.collect()
    output.foreach(tup => println(s"${tup._1} has rank:  ${tup._2} ."))

    Thread.sleep(500000)

    sparkContext.stop()

  }
}
