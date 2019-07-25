package SparkProject_3.UserTags

import SparkProject_3.Project_3_Utils.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsTrade extends Tags{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val provincename = row.getAs[String]("provincename")
    val cityname = row.getAs[String]("cityname")

    //区域标签
    if (StringUtils.isNoneBlank(provincename)){
      list:+=("ZP" + provincename,1)
    }
    if (StringUtils.isNoneBlank(cityname)){
      list:+=("ZC" + cityname,1)
    }
    list
  }
}
