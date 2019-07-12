package Spark_DM

import SparkConfig.ConfigManager
import SparkUtils.JDBCUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object DWD_TO_DM_USER_BASIC {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    val conf = SparkSession
      .builder()
      .enableHiveSupport()
      .appName(SparkConstants.Constant.SPARK_APP_NAME_USER)
      .master(SparkConstants.Constant.SPARK_LOCAL)
      .getOrCreate()

    val sql = ConfigManager.getProper("qfbap_dm.dm_user_basic")
    if (sql == null) {
      LoggerFactory.getLogger("SparkLogger")
        .debug("提交的表名参数异常!请重新设置!")
    }else{
      val df = conf.sql(sql)
      val mysqlTableName = "dm_user_basic"

      val jdbcProp = JDBCUtils.getjdbcProp()._1
      val jdbcUrl = JDBCUtils.getjdbcProp()._2

      df.write.mode(SaveMode.Append).jdbc(jdbcUrl,mysqlTableName,jdbcProp)
      //df.write.mode(SaveMode.Append).insertInto(hiveTableName)
    }
  }
}
