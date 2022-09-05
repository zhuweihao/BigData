package com.zhuweihao

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @Author zhuweihao
 * @Date 2022/8/2 0:53
 * @Description com.zhuweihao
 */
object PageRank1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("pagerank")
    val sparkContext = new SparkContext(sparkConf)

    val point: RDD[String] = sparkContext.textFile("Spark/src/main/resources/page.txt")

    //构成(A,(B,D))的格式，(B,D)是顶点A的出链List
    val pointRDD: RDD[(String, List[String])] = point.map(line => {
      val splits: Array[String] = line.split("-")
      val p: String = splits(0)
      val pList: List[String] = splits(1).split(",").toList
      (p, pList)
    })

    //给上面的(A,(B,D))加上权重，初始为1，构成(A,(B,D),1)的形式
    var point_weight: RDD[(String, List[String], Double)] = pointRDD.map {
      case (p, pList) =>
        (p, pList, 1)
    }

    //规定差值指标为0.0001
    val diff_index: Double = 0.0001

    //初始化变量d，含义是网页PR差值平均值
    var d: Double = 1.0

    while (d > diff_index) {
      point_weight.foreach(println)
      //将权重平均的分给List里面的每个出链
      //先计算平均后的权重，用当前顶点的权重除以当前顶点的出链数
      //然后将上面的权重放在List的每条出链对应的顶点后面，构成Map集合，比如(B,0.5),(D,0.5)
      //利用flatMap展开形成一个个的Map集合
      val new_weight: RDD[(String, Double)] = point_weight.flatMap {
        case (p: String, pList: List[String], weight: Double) =>
          val w: Double = weight / pList.length.toDouble
          val point_weightMap: Map[String, Double] = pList.map(p => (p, w)).toMap
          point_weightMap
      }
      //对上面的一对Map集合按照key值合并，权重相加，形成(顶点，新的权重)的形式，比如(B,1.5)
      //然后拿顶点与一开始的pointRDD关联，重新获取每个顶点的出链List
      //结果类似(A,List(B, D),0.5)，这样又重新构成了一开始的point_weight
      val new_point_weight: RDD[(String, List[String], Double)] = new_weight.reduceByKey(_ + _)
        .join(pointRDD)
        .map {
          case (p: String, (weight: Double, pList: List[String])) =>
            (p, pList, weight)
        }
      //分别构建当前与之前的(顶点,权重)的k-v格式RDD，用来计算每个顶点当前权重和上一次权重的差值
      //当所有顶点的差值的平均值小于差值指标(0.0001)的时候，收敛，即终止while循环
      val current_weight: RDD[(String, Double)] = new_point_weight.map(line => (line._1, line._3))
      val last_weight: RDD[(String, Double)] = point_weight.map(line => (line._1, line._3))
      val cur_last_RDD: RDD[(String, (Double, Double))] = current_weight.join(last_weight)
      d = cur_last_RDD.map {
        case (point: String, (cur_w: Double, last_w: Double)) =>
          val diff_weight: Double = cur_w - last_w
          Math.abs(diff_weight)
      }.sum / cur_last_RDD.count()

      //因为point_weight和new_point_weight的结构一样，所以可以将new_point_weight的内容赋给point_weight
      //在while循环里面，又会对赋值后的新的point_weight重新计算权重
      point_weight = new_point_weight

    }
  }
}
