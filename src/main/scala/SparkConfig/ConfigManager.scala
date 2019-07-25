package SparkConfig

import java.util.Properties

object ConfigManager {
  private val prop = new Properties()
  // 通过类加载器方法来加载指定的配置文件
  try{
    val in_dm = ConfigManager
      .getClass
      .getClassLoader
      .getResourceAsStream("dm.properties")
    //地域
    val in_citys = ConfigManager
      .getClass
      .getClassLoader
      .getResourceAsStream("city_report_forms.properties")
    //运营商
    val in_operators = ConfigManager
      .getClass
      .getClassLoader
      .getResourceAsStream("operator_report_forms.properties")
    //网络
    val in_internets = ConfigManager
      .getClass
      .getClassLoader
      .getResourceAsStream("internet_report_forms.properties")
    //操作设备
    val in_equipments = ConfigManager
      .getClass
      .getClassLoader
      .getResourceAsStream("equipment_report_forms.properties")
    //操作系统
    val in_browsers = ConfigManager
      .getClass
      .getClassLoader
      .getResourceAsStream("browsers_report_forms.properties")
    //jdbc配置参数
    val in_basic = ConfigManager
      .getClass
      .getClassLoader
      .getResourceAsStream("basic.properties")

    prop.load(in_dm)
    prop.load(in_citys)
    prop.load(in_operators)
    prop.load(in_internets)
    prop.load(in_equipments)
    prop.load(in_browsers)
    prop.load(in_basic)
  }catch {
    case e:Exception=>e.printStackTrace()
  }
  /**
    * 获取指定Key的对应Value
    */
  def getProper(key:String):String={
    prop.getProperty(key)
  }
}
