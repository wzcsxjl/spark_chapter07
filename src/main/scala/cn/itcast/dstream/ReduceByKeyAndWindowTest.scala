package cn.itcast.dstream

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用reduceByKeyAndWindow()方法统计3个时间单位内不同字母出现的次数
  */
object ReduceByKeyAndWindowTest {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("ReduceByKeyAndWindowTest").setMaster("local[2]")
    // 2.创建SparkContext对象，它是所有任务计算的源头
    val sc: SparkContext = new SparkContext(sparkConf)
    // 3.设置日志级别
    sc.setLogLevel("WARN")
    // 4.创建StreamingContext对象，需要两个参数，分别为SparkContext和批处理时间间隔
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(1))
    // 5.连接Socket服务，需要socket服务地址、端口号及存储级别（默认的）
    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("node-1", 9999)
    // 6.按空格切分每一行，并将切分的单词出现次数记录为1
    val wordAndOne: DStream[(String, Int)] = dstream.flatMap(_.split(" ")).map(word => (word, 1))
    // 7.调用reduceByKeyAndWindow操作，窗口长度和时间间隔必须是批处理时间间隔的整数倍
    val windowWords: DStream[(String, Int)] = wordAndOne.reduceByKeyAndWindow(
      // 函数
      (a: Int, b: Int) => (a + b),
      // 窗口长度
      Seconds(3),
      // 时间间隔
      Seconds(1)
    )
    // 8.打印输出结果
    windowWords.print()
    // 9.开启流式计算
    ssc.start()
    // 10.让程序一直运行，除非人为干预停止
    ssc.awaitTermination()
  }

}
