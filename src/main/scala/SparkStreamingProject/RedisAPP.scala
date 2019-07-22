package SparkStreamingProject

import org.apache.spark.rdd.RDD

/**
  * Utils
  */
object RedisAPP {
  //总订单量、成功总金额、成功量、充值花费总时长
  def Result01(result01:RDD[(String,List[Double])]){
    result01.foreachPartition(f=>{
      val jedis = JedisConnectionPool.getConnection()
      f.foreach(t=>{
        jedis.hincrBy(t._1,"count",t._2(0).toLong)
        jedis.hincrByFloat(t._1,"money",t._2(1))
        jedis.hincrBy(t._1,"success",t._2(2).toLong)
        jedis.hincrBy(t._1,"time",t._2(3).toLong)
      })
      jedis.close()
    })
  }
  //每分钟订单量
  def Result02(result02:RDD[(String,Double)]){
    result02.foreachPartition(f=>{
      val jedis = JedisConnectionPool.getConnection()
      f.foreach(t=>{
        jedis.incrBy(t._1,t._2.toLong)
      })
      jedis.close()
    })
  }
  //每小时各省份充值失败数据量
  def Result03(result03: RDD[((String, String), Int)]): Unit ={
    result03.foreachPartition(f=>{
      val jdbcconn = JDBCConnectPoolUtils.getConnections()
      f.foreach(t=>{
        val sql = "insert into hourprovince(pro,hour,counts) values('"+t._1._1+"','"+t._1._2+"',"+t._2+")"
        val state = jdbcconn.createStatement()
        state.executeUpdate(sql)
      })
      JDBCConnectPoolUtils.resultConn(jdbcconn)
    })
  }
  //各省订单量top10、成功率
  def Result04(result04: RDD[(String, Double, String)]): Unit = {
    result04.foreachPartition(f=>{
      val jdbcconn = JDBCConnectPoolUtils.getConnections()
      f.foreach(t=>{
        val sql = "insert into province(province,counts,success) values('"+t._1+"','"+t._2+"','"+t._3+"')"
        val state = jdbcconn.createStatement()
        state.executeUpdate(sql)
      })
      JDBCConnectPoolUtils.resultConn(jdbcconn)
    })
  }

}
