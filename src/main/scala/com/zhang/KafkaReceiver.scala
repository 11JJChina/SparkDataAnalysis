package com.zhang

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaReceiver {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaReceiver")
    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.checkpoint("hdfs://bigdata:9000/checkpoint")

    val topic = Map("mydemo2" -> 1)

    val data = KafkaUtils.createStream(ssc, "192.168.88.101:2181", "mygroup", topic,
      StorageLevel.MEMORY_AND_DISK)

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
