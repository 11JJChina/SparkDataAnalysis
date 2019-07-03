package com.zhang.kafka

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaReceiver {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    if (args.length < 2){
      System.err.println(
        s"""
           |DirectKafka  <brokers><topics>
           | <brokers> is a list of one or more kafka brokers
           | <topics> is a list of one or more kafka topics
         """.stripMargin)
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("SparkStreaming-ReceivertKafka")
    val sc = new SparkContext(sparkConf)
    val Array(zkQuorum, group, topics, numThreads) = args
    val ssc = new StreamingContext(sc, Seconds(2))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    // key:topic value:topic对应的分区数
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum, "group.id" -> group,
      "zookeeper.connection.timeout.ms" -> "10000",
      "auto.offset.reset" -> "largest")    // 从最大处读

    val numStreams = 3
    val kafkaStreams = (1 to numStreams).map { _ =>
      KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topicMap, StorageLevel.MEMORY_AND_DISK_SER_2) }  // 存储在executer中方式

    val unifiedStream = ssc.union(kafkaStreams)
    unifiedStream.print()

    val messages = unifiedStream.map(_._2)
    val words = messages.flatMap(_.split(" "))
    val wordcounts = words.map(x => (x, 1L)).
      reduceByKeyAndWindow(_ + _, _ - _,Minutes(1), Seconds(2), 2)
    // 每两秒统计前1分钟数据
    wordcounts.print()
    ssc.start()
    ssc.awaitTermination()

  }

}
