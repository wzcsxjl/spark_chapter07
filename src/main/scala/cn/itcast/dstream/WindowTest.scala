package cn.itcast.dstream

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * window(windowLength, slideInterval)：基于源DStream的窗口进行批次计算后，返回一个新DStream
  */
object WindowTest {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("WindowTest").setMaster("local[2]")
    // 2.创建SparkContext对象，它是所有任务计算的源头
    val sc: SparkContext = new SparkContext(sparkConf)
    // 3.设置日志级别
    sc.setLogLevel("WARN")
    // 4.创建StreamingContext对象，需要两个参数，分别为SparkContext和批处理时间间隔
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(1))
    // 5.连接socket服务，需要socket服务地址、端口号及存储级别（默认的），获取实时的数据流
    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("node-1", 9999)
    // 6.按空格切分每一行
    val words: DStream[String] = dstream.flatMap(_.split(" "))
    // 7.调用window操作，需要两个参数，窗口长度和滑动时间间隔
    val windowWords: DStream[String] = words.window(Seconds(3), Seconds(1))
    // 8.打印输出结果
    windowWords.print()
    // 9.开启流式计算
    ssc.start()
    // 10.让程序一直运行，除非人为干预停止
    ssc.awaitTermination()
  }

}
