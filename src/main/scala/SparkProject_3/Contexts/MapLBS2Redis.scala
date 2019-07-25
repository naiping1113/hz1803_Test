package SparkProject_3.Contexts

import org.apache.spark.sql.SparkSession

object MapLBS2Redis {
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

    val lines = sparksql.read.parquet(inputPath)

  }
}