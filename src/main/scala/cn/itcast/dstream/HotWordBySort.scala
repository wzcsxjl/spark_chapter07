package cn.itcast.dstream

import java.sql.{Connection, DriverManager, Statement}

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 编写Spark Streaming应用程序，实现热词统计排序
  */
object HotWordBySort {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("HotWordBySort").setMaster("local[2]")
    // 2.创建SparkContext对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 3.设置日志级别
    sc.setLogLevel("WARN")
    // 4.创建StreamingContext对象，需要两个参数，分别为SparkContext和批处理时间间隔
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
    // 5.连接socket服务，需要socket服务地址、端口号及存储级别（默认的）
    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("node-1", 9999)
    // 6.通过逗号分隔第一个字段和第二个字段
    val itemPairs: DStream[(String, Int)] = dstream.map(line => (line.split(",")(0), 1))
    // 7.调用reduceByKeyAndWindow操作，需要三个参数
    val itemCount: DStream[(String, Int)] = itemPairs.reduceByKeyAndWindow((v1: Int, v2: Int) => v1 + v2, Seconds(60), Seconds(10))
    // 8.DStream没有sortByKey操作，所以排序用transform实现，false降序，take(3)取前3
    val hotWord: DStream[(String, Int)] = itemCount.transform(itemRDD => {
      val top3: Array[(String, Int)] = itemRDD.map(pair => (pair._2, pair._1)).sortByKey(false).map(pair => (pair._2, pair._1)).take(3)
      // 9.将本地的集合（排名前三的热词组成的集合）转成RDD
      ssc.sparkContext.makeRDD(top3)
    })
    // 10.调用foreachRDD操作，将输出的数据保存到MySQL数据库的表中
    hotWord.foreachRDD(rdd => {
      val url: String = "jdbc:mysql://node-1:3306/spark"
      val user: String = "root"
      val password: String = "123456"
      Class.forName("com.mysql.jdbc.Driver")
      val conn1: Connection = DriverManager.getConnection(url, user, password)
      conn1.prepareStatement("delete from searchKeyWord where 1 = 1").executeUpdate()
      conn1.close()
      rdd.foreachPartition(partitionOfRecords => {
        val url: String = "jdbc:mysql://node-1:3306/spark"
        val user: String = "root"
        val password: String = "123456"
        Class.forName("com.mysql.jdbc.Driver")
        val conn2: Connection = DriverManager.getConnection(url, user, password)
        conn2.setAutoCommit(false)
        val stat: Statement = conn2.createStatement()
        partitionOfRecords.foreach(record => {
          stat.addBatch("insert into searchKeyWord (insert_time, keyword, search_count) values (now(), '" + record._1 + "', '" + record._2 + "')")
        })
        stat.executeBatch()
        conn2.commit()
        conn2.close()
      })
    })
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
