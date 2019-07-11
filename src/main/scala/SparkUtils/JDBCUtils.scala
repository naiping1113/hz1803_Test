package SparkUtils

import java.util.Properties
import SparkConfig.ConfigManager

object JDBCUtils {
  def getjdbcProp():(Properties,String)={
    val prop = new Properties()
    prop.put("user",ConfigManager.getProper("jdbc.user"))
    prop.put("password",ConfigManager.getProper("jdbc.password"))
    prop.put("driver",ConfigManager.getProper("jdbc.driver"))
    val jdbcUrl = ConfigManager.getProper("jdbc.url")
    (prop,jdbcUrl)
  }
}
