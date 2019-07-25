package SparkProject_3.Contexts

import SparkProject_3.Project_3_Utils.TagUtils
import SparkProject_3.UserTags._
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 上下文标签
  */
object TagContext {
  def main(args: Array[String]): Unit = {
    //判断目录是否合法
    if (args.length != 5) {
      println("目录错误，退出程序")
      sys.exit()
    }
    val Array(inputPath,outputPath,dirPath,stopPath,day) = args

    //创建执行入口
    val sparksql = SparkSession
      .builder()
      .appName(SparkConstants.Constant.SPARK_APP_NAME_USER)
      .master(SparkConstants.Constant.SPARK_LOCAL)
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //加载配置文件
    val load = ConfigFactory.load()
    val hbTableName = load.getString("hbase.table.Name")
    val zkHost = load.getString("hbase.zookeeper.host")

    //创建hadoop配置
    val configuration = sparksql.sparkContext.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum",zkHost)

    //创建connection连接
    val hbConn = ConnectionFactory.createConnection(configuration)
    val hbAdmin = hbConn.getAdmin
    if (!hbAdmin.tableExists(TableName.valueOf(hbTableName))) {
      println("可用的表")
      //判断表是否可用
      val hbTableDescriptor = new HTableDescriptor(TableName.valueOf(hbTableName))
      val columnDescriptor = new HColumnDescriptor("tags")
      hbTableDescriptor.addFamily(columnDescriptor)
      hbAdmin.createTable(hbTableDescriptor)
      hbAdmin.close()
      hbConn.close()
    }

    //创建表
    val jobConf = new JobConf(configuration)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,hbTableName)

    //加载字典文件
    val dirfile = sparksql.read.textFile(dirPath)
    val map = dirfile.rdd.filter(f=>f.split("\t").length>=5).map(t=>{
      (t.split("\t",-1)(4),t.split("\t",-1)(1))
    }).collectAsMap()
    val broadcastDIR = sparksql.sparkContext.broadcast(map)

    //加载停用词库
    val stop = sparksql.read.textFile(stopPath)
    val arrkv = stop.collect()
    val broadcastKV = sparksql.sparkContext.broadcast(arrkv)

    //读取文件
    val frame: DataFrame = sparksql.read.parquet(inputPath)
    val results = frame.filter(TagUtils.userIdOne).rdd.map(row=>{
      //获取用户id
      val userID = TagUtils.getAllOneUserId(row)
      //广告
      val adTAG = TagsAD.makeTags(row)
      //APP
      val appTAG = TagsAPP.makeTags(row,broadcastDIR)
      //关键字
      val kvTAG = TagsKeyWords.makeTags(row,broadcastKV)
      //地域
      val regionTAG = TagsRegion.makeTags(row)
      //设备
      val equipTAG = TagsEquipment.makeTags(row)
      //返回对象
      (userID,adTAG++appTAG++kvTAG++regionTAG++equipTAG)
      //聚合
    }).reduceByKey((list1,list2)=>(list1:::list2).groupBy(_._1).mapValues(_.size).toList)

    //解析格式
    val lines = results.map{
      case (userid,userTags)=>{
        val put = new Put(Bytes.toBytes(userid))
        val tags = userTags.map(t=>t._1+":"+t._2).mkString(",")
        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(day),Bytes.toBytes(tags))
        (new ImmutableBytesWritable(),put)
      }
    }
    //存入hbase
    lines.saveAsHadoopDataset(jobConf)
    sparksql.stop()
  }
}