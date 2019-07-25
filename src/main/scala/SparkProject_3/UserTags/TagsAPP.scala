package SparkProject_3.UserTags

import SparkProject_3.Project_3_Utils.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * APP标签
  */
object TagsAPP extends Tags{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val bd = args(1).asInstanceOf[Broadcast[collection.Map[String, String]]]

    //获取appName、appID
    val appName = row.getAs[String]("appname")
    val appID = row.getAs[String]("appid")

    //APP标签
    if (StringUtils.isNoneBlank(appName)) {
      list:+=("APP" + appName,1)
    }else if (StringUtils.isNoneBlank(appID)) {
      list:+=("APP" + bd.value.getOrElse(appID,appID),1)
    }
    list
  }
}
