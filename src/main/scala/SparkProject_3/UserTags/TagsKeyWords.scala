package SparkProject_3.UserTags

import SparkProject_3.Project_3_Utils.Tags
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * 关键字标签
  */
object TagsKeyWords extends Tags{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val bd = args(1).asInstanceOf[Broadcast[Array[String]]]

    //获取关键字
    val keyword = row.getAs[String]("keywords")
    keyword.split("\\|").filter(word=>{
      word.length >= 3 && word.length <= 8 && !bd.value.contains(keyword)
    }).foreach(f=>{
      list:+=("K" + f,1)
    })
    list.distinct
  }
}
