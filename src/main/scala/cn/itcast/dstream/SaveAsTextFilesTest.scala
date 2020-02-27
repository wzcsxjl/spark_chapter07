package cn.itcast.dstream

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用saveAsTextFiles()方法将nc交互界面输入的内容保存在HDFS的/data/root/saveAsTextFiles文件夹下
  * 将每个批次的数据单独保存为一个文件夹
  */
object SaveAsTextFilesTest {

  def main(args: Array[String]): Unit = {
    // 1.设置本地测试环境
    System.setProperty("HADOOP_USER_NAME", "root")
    // 2.创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("SaveAsTextFilesTest").setMaster("local[2]")
    // 3.创建SparkContext对象，它是所有任务计算的源头
    val sc: SparkContext = new SparkContext(sparkConf)
    // 4.设置日志级别
    sc.setLogLevel("WARN")
    // 5.创建StreamingContext，需要两个参数，分别为SparkContext和批处理时间间隔
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
    // 6.连接socket服务，需要socket服务地址、端口号及存储级别（默认的）
    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("node-1", 9999)
    // 7.调用saveAsTextFiles操作，将nc交互界面输出的内容保存到HDFS上
    dstream.saveAsTextFiles("hdfs://node-1:9000/data/root/saveAsTextFiles/staf", "txt")
    // 8.启用流式计算
    ssc.start()
    // 9.让程序一直运行，除非人为干预
    ssc.awaitTermination()
  }

}
