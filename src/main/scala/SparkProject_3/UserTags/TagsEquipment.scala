package SparkProject_3.UserTags

import SparkProject_3.Project_3_Utils.Tags
import org.apache.spark.sql.Row

/**
  * 设备标签(操作系统、网络、运营商)
  */
object TagsEquipment extends Tags{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    //操作系统标签
    val client = row.getAs[Int]("client")
    client match {
      case 1 => list:+=("Android D00010001",1)
      case 2 => list:+=("IOS D00010002",1)
      case 3 => list:+=("WinPhone D00010003",1)
      case _ => list:+=("其他 D00010004",1)
    }
    //网络标签
    val networkmannername = row.getAs[String]("networkmannername")
    networkmannername match {
      case "Wifi" => list:+=("WIFI D00020001",1)
      case "4G" => list:+=("4G D00020002",1)
      case "3G" => list:+=("3G D00020003",1)
      case "2G" => list:+=("2G D00020004",1)
      case _ => list:+=("其他 D00020005",1)
    }
    //运营商标签
    val ispname = row.getAs[String]("ispname")
    ispname match {
      case "移动" => list:+=("移动 D00030001",1)
      case "联通" => list:+=("联通 D00030002",1)
      case "电信" => list:+=("电信 D00030003",1)
      case _ => list:+=("其他 D00020004",1)
    }
    list
  }
}
