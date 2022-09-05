package com.zhuweihao.RDDOperator.actions

import org.apache.spark.{SparkConf, SparkContext}

object foreach {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)
    val rdd = sparkContext.makeRDD(List(1, 2, 4, 3), 2)
    val user = new User()
    //闭包就是一个函数和与其相关的引用环境组合的一个整体(实体)。
    //例如，一个匿名函数 ，该函数引用到到函数外的变量x,那么该函数和变量x整体形成一个闭包
    //RDD算子中传递的函数（函数式编程）存在闭包操作就会进行检测，称为闭包检测，所传递的变量必须可以序列化
    rdd.foreach(
      num => {
        println("age=" + (user.age + num))
      }
    )
    sparkContext.stop()
  }

  //由于Driver端和Executor端发生通信，故User必须序列化，否则会报错NotSerializableException
  class User extends Serializable {
    var age: Int = 30
  }
  /*
  //样例类在编译时，会自动实现可序列化接口
  case class User() {
    var age: Int = 30
  }*/
}
