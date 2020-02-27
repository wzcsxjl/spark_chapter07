package cn.itcast.dstream

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable

/**
  * KafkaUtils.createStream方式创建DStream实现词频统计
  */
object SparkStreaming_Kafka_createStream {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf，并开启wal预定日志，保存数据源
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreaming_Kafka_createDStream").setMaster("local[4]")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
    // 2.创建SparkContext
    val sc: SparkContext = new SparkContext(sparkConf)
    // 3.设置日志级别
    sc.setLogLevel("WARN")
    // 4.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
    // 5.设置checkpoint
    ssc.checkpoint("./Kafka_Receiver")
    // 6.定义zk地址
    val zkQuorum: String = "node-1:2181,node-2:2181,node-3:2181"
    // 7.定义消费者组
    val groupId: String = "spark_receiver"
    // 8.定义topic相关信息
    // Map[String, Int]这里value不是topic分区，而是topic中每个分区被N个线程消费
    val topics: Map[String, Int] = Map("kafka_spark" -> 1)
    // 9.通过高级API方式将Kafka跟SparkStreaming整合
    // 这个时候相当于同时开启3个receiver接收数据
    val receiverDStream: immutable.IndexedSeq[ReceiverInputDStream[(String, String)]] = (1 to 3).map(x => {
      val stream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topics)
      stream
    })
    // 10.使用ssc中的union方法合并所有的receiver中的数据，Kafka中消息是以(K, V)形式存在的，通常不传K，值为Null
    val unionDStream: DStream[(String, String)] = ssc.union(receiverDStream)
    // 11.SparkStreaming获取topic中的数据；map(_._2)是map(t => t._2)的简写
    val topicData: DStream[String] = unionDStream.map(_._2)
    // 12.按空格切分每一行，并将切分的单词出现次数记录为1；flatMap(_.split(" ")).map((_, 1))是flatMap(line => line.split(" ")).map(word => (word, 1))的简写
    val wordAndOne: DStream[(String, Int)] = topicData.flatMap(_.split(" ")).map((_, 1))
    // 13.统计单词在全局中出现的次数；reduceByKey(_ + _)是reduceByKey((a: Int, b:Int) => (a + b))的简写
    val result: DStream[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    // 14.打印输出结果
    result.print()
    // 15.开启流式计算
    ssc.start()
    ssc.awaitTermination()
  }

}
