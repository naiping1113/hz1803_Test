package Spark_DM

import SparkConfig.ConfigManager
import SparkUtils.JDBCUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object DM_USER_VISIT {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    val conf = SparkSession
      .builder()
      .enableHiveSupport()
      .appName(SparkConstants.Constant.SPARK_APP_NAME_USER)
      .master(SparkConstants.Constant.SPARK_LOCAL)
      .getOrCreate()
    val sql = ConfigManager.getProper(args(0))
    if (sql == null) {
      LoggerFactory.getLogger("SparkLogger")
        .debug("提交的表名参数异常!请重新设置!")
    }else{
      //val finalsql = sql.replace("?",args(1))
      val df = conf.sql(sql)
      val mysqlTableName = args(0).split("\\.")(1)
      val hiveTableName = args(0)
      val jdbcProp = JDBCUtils.getjdbcProp()._1
      val jdbcUrl = JDBCUtils.getjdbcProp()._2
      df.coalesce(1).write.mode(SaveMode.Append).jdbc(jdbcUrl,mysqlTableName,jdbcProp)
      //df.write.mode(SaveMode.Append).insertInto(hiveTableName)
    }
  }
}
