package com.zhuweihao.SparkStreaming

import cn.hutool.extra.ftp.{Ftp, FtpMode}
import org.apache.commons.io.FileUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.json4s.DateFormat

import java.io.{File, FileInputStream, FileWriter, InputStream, PrintWriter}
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

/**
 * @Author zhuweihao
 * @Date 2022/7/8 23:44
 * @Description com.zhuweihao.scala.SparkStreaming
 */
object kafka {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local")
    val streamingContext = new StreamingContext(sparkConf, Seconds(15))

    //读取配置文件
    /*val properties: Properties = new Properties()
    properties.load(this.getClass.getClassLoader.getResourceAsStream("properties.properties"))
    properties.getProperty("BOOTSTRAP_SERVERS")*/
    val properties: Properties = new Properties()
    properties.load(new FileInputStream("properties.properties"))
    val BOOTSTRAP_SERVERS = properties.getProperty("BOOTSTRAP_SERVERS")
    val GROUP_ID = properties.getProperty("GROUP_ID")
    val key_deserializer = properties.getProperty("key.deserializer")
    val value_deserializer = properties.getProperty("value.deserializer")
    val topic = properties.getProperty("topic")
    val host = properties.getProperty("host")
    val port = properties.getProperty("port")
    val user = properties.getProperty("user")
    val passerword = properties.getProperty("passerword")
    val pathname = properties.getProperty("pathname")
    //kafka相关配置
    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> BOOTSTRAP_SERVERS,
      ConsumerConfig.GROUP_ID_CONFIG -> GROUP_ID,

      "key.deserializer" -> key_deserializer,
      "value.deserializer" -> value_deserializer
    )
    val kafkaData: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(topic), kafkaParams)
    )
    val result: DStream[String] = kafkaData.map(_.value())
    result.foreachRDD(
      rdd => rdd.foreachPartition(
        iter => {
          val partitionId: Int = TaskContext.getPartitionId()
          val date: Any = new Date()
          val time: String = new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(date)
          val ftpClient = new Ftp(host, port.toInt, user, passerword)
          val file: File = new File(pathname + "kafkaData-" + partitionId + "-" + time + ".txt")
          if (!file.exists()) {
            file.createNewFile()
          }
          ftpClient.setMode(FtpMode.Passive)
          val strings: util.ArrayList[String] = new util.ArrayList[String]()
          while (iter.hasNext) {
            val str: String = iter.next()
            strings.add(str)
            if (strings.size() > 10) {
              FileUtils.writeLines(file, "utf-8", strings, true)
              strings.clear()
            }
          }
          if (strings.size() > 0) {
            FileUtils.writeLines(file, "utf-8", strings, true)
          }
          ftpClient.cd(".")
          val bool: Boolean = ftpClient.upload(".", file)
          println("upload:" + bool)
          ftpClient.close()
        }
      )
    )


    streamingContext.start()
    streamingContext.awaitTermination()

  }
}

