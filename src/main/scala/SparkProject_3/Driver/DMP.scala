package SparkProject_3.Driver

import SparkConfig.ConfigManager
import org.apache.spark.sql.{SaveMode, SparkSession}
import SparkProject_3.Project_3_Utils.StringUtil
import SparkUtils.JDBCUtils
import org.apache.log4j.{Level, Logger}

object DMP {
  //Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("目录错误，退出程序")
      sys.exit()
    }
    val Array(inputPath,outputPath) = args

    //创建执行入口
    val sparksql = SparkSession
      .builder()
      .appName(SparkConstants.Constant.SPARK_APP_NAME_USER)
      .master(SparkConstants.Constant.SPARK_LOCAL)
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //转换为dataframe
    val file = sparksql.read.textFile(inputPath)
    val lines = file.rdd.map(t=>t.split(",",-1)).filter(_.length >= 85)
    val df = sparksql.createDataFrame(lines.map(arr=>{
      getValues(arr(0),
      StringUtil.String2Int(arr(1)),
      StringUtil.String2Int(arr(2)),
      StringUtil.String2Int(arr(3)),
      StringUtil.String2Int(arr(4)),
      arr(5),
      arr(6),
      StringUtil.String2Int(arr(7)),
      StringUtil.String2Int(arr(8)),
      StringUtil.String2Double(arr(9)),
      StringUtil.String2Double(arr(10)),
      arr(11),
      arr(12),
      arr(13),
      arr(14),
      arr(15),
      arr(16),
      StringUtil.String2Int(arr(17)),
      arr(18),
      arr(19),
      StringUtil.String2Int(arr(20)),
      StringUtil.String2Int(arr(21)),
      arr(22),
      arr(23),
      arr(24),
      arr(25),
      StringUtil.String2Int(arr(26)),
      arr(27),
      StringUtil.String2Int(arr(28)),
      arr(29),
      StringUtil.String2Int(arr(30)),
      StringUtil.String2Int(arr(31)),
      StringUtil.String2Int(arr(32)),
      arr(33),
      StringUtil.String2Int(arr(34)),
      StringUtil.String2Int(arr(35)),
      StringUtil.String2Int(arr(36)),
      arr(37),
      StringUtil.String2Int(arr(38)),
      StringUtil.String2Int(arr(39)),
      StringUtil.String2Double(arr(40)),
      StringUtil.String2Double(arr(41)),
      StringUtil.String2Int(arr(42)),
      arr(43),
      StringUtil.String2Double(arr(44)),
      StringUtil.String2Double(arr(45)),
      arr(46),
      arr(47),
      arr(48),
      arr(49),
      arr(50),
      arr(51),
      arr(52),
      arr(53),
      arr(54),
      arr(55),
      arr(56),
      StringUtil.String2Int(arr(57)),
      StringUtil.String2Double(arr(58)),
      StringUtil.String2Int(arr(59)),
      StringUtil.String2Int(arr(60)),
      arr(61),
      arr(62),
      arr(63),
      arr(64),
      arr(65),
      arr(66),
      arr(67),
      arr(68),
      arr(69),
      arr(70),
      arr(71),
      arr(72),
      StringUtil.String2Int(arr(73)),
      StringUtil.String2Double(arr(74)),
      StringUtil.String2Double(arr(75)),
      StringUtil.String2Double(arr(76)),
      StringUtil.String2Double(arr(77)),
      StringUtil.String2Double(arr(78)),
      arr(79),
      arr(80),
      arr(81),
      arr(82),
      arr(83),
      StringUtil.String2Int(arr(84)))
    }))

    //创建临时表
    val jdbcProp = JDBCUtils.getjdbcProp()._1
    df.createTempView("dmp")

    /**
      * 指标报表
      */
    //    //地域
//    val sql = ConfigManager.getProper("city")
//    //运营商
//    val sql = ConfigManager.getProper("operator")
//    //网络
//    val sql = ConfigManager.getProper("internet")
//    //设备
//    val sql = ConfigManager.getProper("equipment")
    //操作系统
    //val sql = ConfigManager.getProper("browser")

    //sparksql.sql(sql).show()

    /**
      * 保存
      */
    //df.write.parquet(outputPath)
    //df.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/qfbap","dmp",jdbcProp)
    //df.write.json("hdfs://centos2:9000/project_3/output")

    /**
      * 每个省份城市的分布率
      */
    //val citycount = sparksql.sql("select provincename,cityname,count(*) as counts from dmp group by provincename,cityname")
    //citycount.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/qfbap","citycounts",jdbcProp)
    //citycount.write.partitionBy("provincename","cityname")
    sparksql.stop()
  }
}

case class getValues(
                      sessionid: String,
                      advertisersid: Int,
                      adorderid: Int,
                      adcreativeid: Int,
                      adplatformproviderid: Int,
                      sdkversion: String,
                      adplatformkey: String,
                      putinmodeltype: Int,
                      requestmode: Int,
                      adprice: Double,
                      adppprice: Double,
                      requestdate: String,
                      ip: String,
                      appid: String,
                      appname: String,
                      uuid: String,
                      device: String,
                      client: Int,
                      osversion: String,
                      density: String,
                      pw: Int,
                      ph: Int,
                      longs: String,
                      lat: String,
                      provincename: String,
                      cityname: String,
                      ispid: Int,
                      ispname: String,
                      networkmannerid: Int,
                      networkmannername: String,
                      iseffective: Int,
                      isbilling: Int,
                      adspacetype: Int,
                      adspacetypename: String,
                      devicetype: Int,
                      processnode: Int,
                      apptype: Int,
                      district: String,
                      paymode: Int,
                      isbid: Int,
                      bidprice: Double,
                      winprice: Double,
                      iswin: Int,
                      cur: String,
                      rate: Double,
                      cnywinprice: Double,
                      imei: String,
                      mac: String,
                      idfa: String,
                      openudid: String,
                      androidid: String,
                      rtbprovince: String,
                      rtbcity: String,
                      rtbdistrict: String,
                      rtbstreet: String,
                      storeurl: String,
                      realip: String,
                      isqualityapp: Int,
                      bidfloor: Double,
                      aw: Int,
                      ah: Int,
                      imeimd5: String,
                      macmd5: String,
                      idfamd5: String,
                      openudidmd5: String,
                      androididmd5: String,
                      imeisha1: String,
                      macsha1: String,
                      idfasha1: String,
                      openudidsha1: String,
                      androididsha1: String,
                      uuidunknow: String,
                      userid: String,
                      iptype: Int,
                      initbidprice: Double,
                      adpayment: Double,
                      agentrate: Double,
                      lomarkrate: Double,
                      adxrate: Double,
                      title: String,
                      keywords: String,
                      tagid: String,
                      callbackdate: String,
                      channelid: String,
                      mediatype: Int)