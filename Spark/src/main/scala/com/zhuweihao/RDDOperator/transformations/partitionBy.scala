package com.zhuweihao.RDDOperator.transformations

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object partitionBy {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4), 2)
    val mapRDD = rdd.map((_, 1))
    /**
     * RDD=>PairRDDFunctions
     * RDD类型的mapRDD调用了PairRDDFunctions里面的方法
     * 隐式转换（二次编译）
     * class RDD里面有一个RDD伴生对象，里面存在一个隐式函数可以实现上述类型转换
     * implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
     * (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
     * new PairRDDFunctions(rdd)
     * }
     */
    val newRDD = mapRDD.partitionBy(new HashPartitioner(4))
    for (i <- 0 to 3) {
      println(newRDD.mapPartitionsWithIndex(
        (index, iter) => {
          if (index == i) {
            iter
          } else {
            Nil.iterator
          }
        }
      ).collect().mkString(","))
    }


    sparkContext.stop()
  }
}
