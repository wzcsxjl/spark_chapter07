package cn.itcast.dstream

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreaming_Kafka_createDirectStream {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreaming_Kafka_createDirectStream").setMaster("local[2]")
    // 2.创建SparkContext
    val sc: SparkContext = new SparkContext(sparkConf)
    // 3.设置日志级别
    sc.setLogLevel("WARN")
    // 4.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
    // 5.设置checkpint
    ssc.checkpoint("./Kafka_Direct")
    // 6.配置Kafka相关参数（metadata.broker.list为老版本kafka集群地址）
    // 新版本可将"metadata.broker.list" -> "node-1:9092,node-2:9092,node-3:9092"替换为"bootstrap.servers" -> "node-1:9092,node-2:9092,node-3:9092"
    val kafkaParams = Map("bootstrap.servers" -> "node-1:9092,node-2:9092,node-3:9092",
      "group.id" -> "spark_direct")
    // 7.定义topic
    val topics: Set[String] = Set("kafka_direct0")
    // 8.通过低级API方式将Kafka与SparkStreaming进行整合
    /*
    createDirectStream[
    K: ClassTag,                    // type of Kafka message key
    V: ClassTag,                    // type of Kafka message value
    KD <: Decoder[K]: ClassTag,     // type of Kafka message value decoder
    VD <: Decoder[V]: ClassTag] (   // type of Kafka message value decoder
      ssc: StreamingContext,
      kafkaParams: Map[String, String],
      topics: Set[String]
  ): InputDStream[(K, V)]           // DStream of (Kafka message key, Kafka message value)
     */
    val dstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    // 9.获取Kafka中topic中的数据
    val topicData: DStream[String] = dstream.map(_._2)
    // 10.按空格键切分每一行，并将切分的单词出现次数记录为1
    val wordAndOne: DStream[(String, Int)] = topicData.flatMap(_.split(" ")).map((_, 1))
    // 11.统计单词在全局中出现的次数
    val result: DStream[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    // 12.打印输出结果
    result.print()
    // 13.开启流式计算
    ssc.start()
    ssc.awaitTermination()
  }

}
