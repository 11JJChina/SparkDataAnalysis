package com.zhang

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaDirector {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaDirector")
    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.checkpoint("hdfs://bigdata:9000/checkpoint")

    val topics = Set("mydemo2")

    val kafkaParams = Map[String,String]("metadata.broker.list" -> "192.168.128.111:9092")

    val data = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)

    val updateFunc = (curVal: Seq[Int], preVal: Option[Int]) => {
      var total = curVal.sum
      var previous = preVal.getOrElse(0)
      Some(total + previous)
    }

    val result = data.map(_._2).flatMap(_.split(" ")).map(word => (word, 1))
      .updateStateByKey(updateFunc).print()

    ssc.start()
    ssc.awaitTermination()

  }

}
