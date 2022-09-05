package com.zhuweihao.SparkStreaming

import cn.hutool.extra.ftp.{Ftp, FtpMode}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.FileUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.io.File
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

/**
 * @Author zhuweihao
 * @Date 2022/7/22 0:27
 * @Description com.zhuweihao.SparkStreaming
 */
object newKafka {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local")
    val streamingContext = new StreamingContext(sparkConf, Seconds(15))


    //读取配置文件
    val config: Config = ConfigFactory.load()

    val BOOTSTRAP_SERVERS = config.getString("BOOTSTRAP_SERVERS")
    val GROUP_ID = config.getString("GROUP_ID")
    val key_deserializer = config.getString("key.deserializer")
    val value_deserializer = config.getString("value.deserializer")
    val topic = config.getString("topic")
    val host = config.getString("host")
    val port = config.getString("port")
    val user = config.getString("user")
    val passerword = config.getString("passerword")
    val pathname = config.getString("pathname")
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
