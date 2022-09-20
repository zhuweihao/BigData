package com.zhuweihao.WindowFunction

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author zhuweihao
 * @Date 2022/9/13 15:42
 * @Description com.zhuweihao.WindowFunction
 */
object test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("windowfunction")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sqlContext: SQLContext = sparkSession.sqlContext

    val data = Array(
      ("lili", "ml", 90),
      ("lucy", "ml", 85),
      ("cherry", "ml", 80),
      ("terry", "ml", 85),
      ("tracy", "cs", 82),
      ("tony", "cs", 86),
      ("tom", "cs", 75)
    )

    val schemas = Seq("name", "subject", "score")
    val df = sparkSession.createDataFrame(data).toDF(schemas: _*)
    df.createOrReplaceTempView("person_subject_score")

    val sqltext = "select name, subject, score, rank() over (partition by subject order by score desc) as rank from person_subject_score";
    val ret = sqlContext.sql(sqltext)
    ret.show()
    sqlContext.sql(sqltext).explain(mode="formatted")

    Thread.sleep(500000)
  }
}
