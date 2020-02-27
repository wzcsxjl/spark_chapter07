package cn.itcast.dstream

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 实现一行语句分隔成多个单词的功能
  */
object TransformTest {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf对象，用于配置Spark环境
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("TransformTest").setMaster("local[2]")
    // 2.创建SparkContext对象，它是所有任务计算的源头，用于操作Spark集群
    val sc: SparkContext = new SparkContext(sparkConf)
    // 3.设置日志输出级别
    sc.setLogLevel("WARN")
    // 4.创建StreamingContext对象（需要两个参数，分别为SparkContext和批处理时间间隔），用于创建DStream对象
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
    // 5.连接socket服务，需要socket服务地址、端口号及存储级别（默认的），获取实时的流数据
    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("node-1", 9999)
    // 6.使用RDD-to-RDD函数，返回新的DStream对象（即words）,并按空格切分
    val words: DStream[String] = dstream.transform(rdd => rdd.flatMap(_.split(" ")))
    // 7.打印输出结果
    words.print()
    // 8.开启流式计算
    ssc.start()
    // 9.用于保持程序一直运行，除非人为干预停止
    ssc.awaitTermination()
  }

}
