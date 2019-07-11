package constants

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object Constant {
  def main(args: Array[String]): Unit = {
    val conf = SparkSession
      .builder()
      .enableHiveSupport()
      .appName(this.getClass.getName)
      .master("local")
      .getOrCreate()
    val sql = ""
    if (sql == null) {
      LoggerFactory.getLogger("SparkLogger").debug("提交的表名参数异常!请重新设置!")
    }else{
      val finalsql = sql.replace("?",args(1))
      val df = conf.sql(finalsql)
      val mysqlTableName = args(0).split("\\.")(1)
      val hiveTableName = args(0)
      val jdbcurl =
      df.write.mode(SaveMode.Append).jdbc()
      df.write.mode(SaveMode.Append).insertInto(hiveTableName)
    }
  }
}
