package cn.itcast.spark.day7

import kafka.serializer.StringDecoder
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaManager
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * Created by root on 2016/5/24.
  */
object PlugsScanner {

  def processRdd(rdd: RDD[(String, Long)]): Unit = {
    lazy val jedis: Jedis = JedisConnectionPool.getConnection()
    rdd.foreachPartition(it => {
      it.foreach(t => {
        val account = t._1
        val time = System.currentTimeMillis()
        println(account)
        jedis.set(account, "")
      })
    })
    jedis.close()
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    val Array(brokers, topics, groupId) = Array("node-1.itcast.cn:9092,node-1.itcast.cn:9092,node-1.itcast.cn:9092", "gamelogs", "g1")
    //val Array(zkQuorum, group, topics, numThreads) = Array("node-1.itcast.cn:2181,node-2.itcast.cn:2181,node-3.itcast.cn:2181", "g3", "gamelogs", "2")
    val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    val conf = new SparkConf().setAppName("PlugsScanner").setMaster("local[2]")
    conf.set("spark.streaming.kafka.maxRatePerPartition", "100")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Milliseconds(10000))

//    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
//    val dstream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest"
    )

    val km = new KafkaManager(kafkaParams)

    val dstream = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)


    val lines = dstream.map(_._2)
    val fields = lines.map(_.split("\t"))

    val ds1 = fields.filter(field => {
      field(3).equals("11") && field(8).equals("强效太阳水")
    })

    val ds2 = ds1.map(f => (f(7), dateFormat.parse(f(12)).getTime)).groupByKeyAndWindow(Milliseconds(30000), Milliseconds(10000)).filter(_._2.size > 2)

    val ds3 = ds2.mapValues(it => {
      val list = it.toList.sorted
      val size = list.size
      val firt = list(0)
      val last = list(size - 1)
      (last - firt) / size
    })
    //ds3.print()
    val ds4 = ds3.filter(_._2 < 1000L)

    ds4.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        // 先处理消息
        processRdd(rdd)
        //km.updateZKOffsets(rdd)
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
