package cn.itcast.dstream

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 实现词频统计
  */
object UpdateStateByKeyTest {

  /**
    * newValues表示当前批次汇总成的(K, V)中相同K的所有V
    * runningCount表示历史的所有相同的key的value总和
    *
    * @param newValues
    * @param runningCount
    * @return
    */
  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    val newCount: Int = runningCount.getOrElse(0) + newValues.sum
    Some(newCount)
  }

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf对象，用于配置Spark环境
    val sparkConf: SparkConf = new SparkConf().setAppName("UpdateStateByKeyTest").setMaster("local[2]")
    // 2.创建SparkContext对象，它是所有任务计算的源头，用于操作Spark集群
    val sc: SparkContext = new SparkContext(sparkConf)
    // 3.设置日志级别
    sc.setLogLevel("WARN")
    // 4.创建StreamingContext，需要两个参数，分别为SparkContext和批处理的时间间隔，用于创建DStream对象
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
    // 5.配置检查点目录，使用updateStateByKey()方法必须配置检查点目录
    ssc.checkpoint("./")
    // 6.使用StreamingContext对象连接socket服务，创建ReceiverInputDStream对象，需要socket服务地址、端口号及存储级别（此处为默认的）
    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("node-1", 9999)
    // 7.按空格切分每一行，并将切分出来的单词出现的次数记为1
    val wordAndOne: DStream[(String, Int)] = dstream.flatMap(_.split(" ")).map(word => (word, 1))
    // 8.调用updateStateByKey操作，统计单词在全局中出现的次数
    val result: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunction)
    // 9.打印输出结果
    result.print()
    // 10.开启流式计算
    ssc.start()
    // 11.用于保持程序运行，除非被干预停止
    ssc.awaitTermination()
  }

}
