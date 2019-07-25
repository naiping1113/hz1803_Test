package SparkProject_2.Driver

import java.lang

import SparkProject_2.ConnectUtils.{JedisConnectionPool, JedisOffset}
import SparkProject_2.Project_2_Utils.LogUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Redis管理Offset
  */
object KafkaRedisOffset {
  def main(args: Array[String]): Unit = {
    //System.setProperty("HADOOP_USER_NAME","root")
    val conf = new SparkConf()
      .setAppName(SparkConstants.Constant.SPARK_APP_NAME_USER)
      .setMaster(SparkConstants.Constant.SPARK_LOCAL)
      // 设置没秒钟每个分区拉取kafka的速率
      .set("spark.streaming.kafka.maxRatePerPartition","100")
      // 设置序列化机制
      .set("spark.serlizer","org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf,Seconds(10))
    val file = ssc.sparkContext.textFile("File:///D:\\测试文件\\hive练习\\city.txt")
    val lines = file.map(x=>(x.split(" ")(0),x.split(" ")(1))).collect().toMap
    val broad = ssc.sparkContext.broadcast(lines)
    // 配置参数
    // 配置基本参数
    // 组名
    val groupId = "hz006"
    // topic
    val topic = "hz1803"
    // 指定Kafka的broker地址（SparkStreaming程序消费过程中，需要和Kafka的分区对应）
    val brokerList = "192.168.198.102:9092"
    // 编写Kafka的配置参数
    val kafkas = Map[String,Object](
      "bootstrap.servers"->brokerList,
      // kafka的Key和values解码方式
      "key.deserializer"-> classOf[StringDeserializer],
      "value.deserializer"-> classOf[StringDeserializer],
      "group.id"->groupId,
      // 从头消费
      "auto.offset.reset"-> "earliest",
      // 不需要程序自动提交Offset
      "enable.auto.commit"-> (false:lang.Boolean)
    )
    // 创建topic集合，可能会消费多个Topic
    val topics = Set(topic)
    /**
      * 第一步获取Offset
      * 第二步通过Offset获取Kafka数据
      * 第三步提交更新Offset
      */
    // 获取Offset
    var fromOffset:Map[TopicPartition,Long] = JedisOffset(groupId)
    // 判断一下有没数据
    val stream :InputDStream[ConsumerRecord[String,String]] =
      if(fromOffset.size == 0){
        KafkaUtils.createDirectStream(ssc,
          // 本地策略
          // 将数据均匀的分配到各个Executor上面
          LocationStrategies.PreferConsistent,
          // 消费者策略
          // 可以动态增加分区
          ConsumerStrategies.Subscribe[String,String](topics,kafkas)
        )
      }else{
        // 不是第一次消费
        KafkaUtils.createDirectStream(ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Assign[String,String](fromOffset.keys,kafkas,fromOffset)
        )
      }
    stream.foreachRDD({
      rdd=>
        val offestRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        /**
          * 业务实现
          */
        val utils = new LogUtils
        val value = rdd.map(_.value())
        //value.foreach(println)
        utils.getJsonValues(value,broad)

        // 将偏移量进行更新
        val jedis = JedisConnectionPool.getConnection()
        for (or<-offestRange){
          jedis.hset(groupId,or.topic+"-"+or.partition,or.untilOffset.toString)
        }
        jedis.close()
    })
    // 启动
    ssc.start()
    ssc.awaitTermination()
  }
}
