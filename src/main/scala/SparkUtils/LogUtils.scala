package SparkUtils

import SparkStreamingProject.RedisAPP
import com.alibaba.fastjson.JSON
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

class LogUtils extends Serializable {
  def getJsonValues(rdd:RDD[String],broad:Broadcast[Map[String, String]]): Unit ={
    val baseData = rdd.map(x=>JSON.parseObject(x))
      .filter(_.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq"))
      .map(t=>{
        //成功充值金额
        val money:Double = if (t.getString("bussinessRst").equals("0000")) t.getDouble("chargefee") else 0.0
        //是否成功
        val success = if (t.getString("bussinessRst").equals("0000")) 1 else 0
        //充值开始时间
        val starttime = t.getString("requestId")
        //充值结束时间
        val stoptime = t.getString("receiveNotifyTime")
        //转换为时间戳
        val begin = SparkUtils.TimeUtils.tranTimeToLong(starttime.substring(0,17))
        val after = SparkUtils.TimeUtils.tranTimeToLong(stoptime)
        val days = starttime.substring(0,8)
        val hours = starttime.substring(0,10)
        val minis = starttime.substring(0,12)
        //订单充值花费时间
        val usetime = after-begin
        //获取省份名称
        val province = broad.value.get(t.getString("provinceCode")).get

        /**
          * 1、日
          * 2、小时
          * 3、分钟
          * 4、省份
          * 5、需要累加批次参数（订单量、成功充值金额、成功量、充值时长）
          */
        (days,hours,minis,province,List[Double](1,money,success,usetime))
      }).cache()
    //每天总订单量、成功总金额、成功量、充值花费总时长
    val result01 = baseData.map(t=>(t._1,t._5)).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    })
    RedisAPP.Result01(result01)

    //每分钟订单量
    val result02 = baseData.map(t=>(t._3,t._5(0))).reduceByKey(_+_)
    RedisAPP.Result02(result02)

    //每小时各省份充值失败数据量
    val result03 = baseData.map(t=>((t._4,t._2),(t._5(0)-t._5(2)).toInt)).reduceByKey(_+_)
    RedisAPP.Result03(result03)

    //各省订单量top10、成功率(保留一位小数点)
    val result04 = baseData.map(t=>(t._4,t._5)).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    }).sortBy(_._2(0),false).map(x=>{
      val successPercent = x._2(2)/x._2(0)
      (x._1,x._2(0),successPercent.formatted("%.1f"))
    })
    RedisAPP.Result04(result04)

    //每小时成功量和金额

  }
}
