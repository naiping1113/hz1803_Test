package SparkProject_3.UserTags

import SparkProject_3.Project_3_Utils.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 广告标签
  */
object TagsAD extends Tags{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val adType = row.getAs[Int]("adspacetype")
    //获取广告类型
    adType match {
      case v if v > 9 => list:+=("LC" + v,1)
      case v if v <= 9 && v > 0 => list:+=("LC0" + v,1)
    }
    //广告名称标签
    val adName = row.getAs[String]("adspacetypename")
    if (StringUtils.isNoneBlank(adName)) {
      list:+=("LN" + adName,1)
    }
    //渠道标签
    val channel = row.getAs[Int]("adplatformproviderid")
    list:+=("CN" + channel,1)
    list
  }
}
