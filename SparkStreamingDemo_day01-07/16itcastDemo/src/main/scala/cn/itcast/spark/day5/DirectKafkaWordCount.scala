package cn.itcast.spark.day5

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.{KafkaManager, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object DirectKafkaWordCount {

  /*  def dealLine(line: String): String = {
      val list = line.split(',').toList
  //    val list = AnalysisUtil.dealString(line, ',', '"')// 把dealString函数当做split即可
      list.get(0).substring(0, 10) + "-" + list.get(26)
    }*/

  def processRdd(rdd: RDD[(String, String)]): Unit = {
    val lines = rdd.map(_._2)
    val words = lines.map(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.foreach(println)
  }

  def main(args: Array[String]) {

    //=======================1
    /*
    * 当你的这个jar包传进来的变量小于3，它就会提示你
    * */
    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: DirectKafkaWordCount <brokers> <topics> <groupid>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |  <groupid> is a consume group
           |
        """.stripMargin)
      System.exit(1)
    }


    //======================2
    /*
    * 设置日志的级别
    * */
    Logger.getLogger("org").setLevel(Level.WARN)

    //==========================3
    /*
    * 接受的参数
    * */
    val Array(brokers, topics, groupId) = args


    //==========================4
    /*
    * 设置spark程序的参数
    * */
    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    sparkConf.setMaster("local[*]")

    //设置速率。要求Executor在1s中只能拉取5条数据
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "5")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


    //===========================5
    /*
    * 创建一个streamingContext对象
    * */
    //设置每2s产生一个RDD
    val ssc = new StreamingContext(sparkConf, Seconds(2))


    //==========================6
    //这个sparkStreaming程序可以同时从多个topic当中提取数据
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest"
    )


    //==============================7
    /*
    * 使用直连的方式读取数据
    * */
    //这个KafkaManager
    val km = new KafkaManager(kafkaParams)


    //第二个string是Executor直接链接Kafka的broker主机的Partition
    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    //===============================8
    /*
    * 处理信息
    * */
    messages.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        // 先处理消息
        processRdd(rdd)
        // 再更新offsets
        km.updateZKOffsets(rdd)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}