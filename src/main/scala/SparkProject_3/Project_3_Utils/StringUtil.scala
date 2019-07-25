package SparkProject_3.Project_3_Utils

/**
  * 格式转换
  */
object StringUtil {
  def String2Int(str:String):Int={
    try{
      str.toInt
    }catch {
      case _ : Exception => 0
    }
  }

  def String2Double(str:String):Double={
    try{
      str.toDouble
    }catch {
      case _ : Exception => 0.0
    }
  }
}
