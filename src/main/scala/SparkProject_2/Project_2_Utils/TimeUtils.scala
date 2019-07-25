package SparkProject_2.Project_2_Utils

import java.text.SimpleDateFormat

object TimeUtils {
  def tranTimeToLong(tm:String) :Long={
    val fm = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    val dt = fm.parse(tm)
    val aa = fm.format(dt)
    val tim: Long = dt.getTime()
    tim
  }
}
