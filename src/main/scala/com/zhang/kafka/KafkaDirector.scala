package com.zhang.kafka

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaDirector {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaDirector")
    val Array(brokers, topics) = args
    val ssc = new StreamingContext(conf, Seconds(5))
    val topicSet = topics.split(",").toSet

    ssc.checkpoint("hdfs://bigdata:9000/checkpoint")

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)

    var offsetRanges = Array.empty[OffsetRange]

    directKafkaStream.print()

    directKafkaStream.transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }.map(_._2)
      .flatMap(_.split(" "))
      .map(x => (1, 1L))
      .reduceByKey(_ + _)
      .foreachRDD {
        rdd =>
          for (o <- offsetRanges) {
            println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
          }
          rdd.take(10).foreach(println)
      }


    ssc.start()
    ssc.awaitTermination()

  }

}
