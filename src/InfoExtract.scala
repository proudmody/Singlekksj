import java.util.Date
import java.text.SimpleDateFormat
import java.util.Timer
import java.util.TimerTask

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Get, HBaseAdmin, HTable, Put, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ListBuffer, Map}
import scala.math._

object InfoExtract {
  def dis(x1:Double,y1:Double,x2:Double,y2:Double): Double =
  {
    val detaX = abs(x1 - x2)
    val detaY = abs(y1 - y2)
    val dis = ((detaX + detaY) * Pi *6371 / 180 )
    dis
  }
  def time_diff(s1:String,s2:String):Long ={
    val df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //println(s1+"  "+s2)
    val date1 = df1.parse(s1)
    val date2= df1.parse(s2)
    // println(date1.getTime+"\t" + date2.getTime)
    val dis = abs(date1.getTime -date2.getTime)/1000
    if (dis !=0) dis+20
    else dis+10000000
  }
  case class Record(sj:String, sbbh:String)
  def rec2Rec(rec:record): Record =
  {
    Record(rec.JGSJ,rec.SBBH)
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: An interval of time (begin,end)")
      System.exit(1)
    }
    //spark configure
    val jar = ListBuffer[String]()
    jar += "hdfs://linux1:9000/jar/Singlekksj.jar"
    val sparkConf = new SparkConf().
      setMaster("spark://linux1:7077").
      setAppName("KakouInfoExtract").
      set("spark.driver.memory", "1g").
      set("spark.executor.memory", "4g").
      set("spark.cores.max", "12").
      set("spark.testing.memory", "2147480000")
    //create SparkContext and sql
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    //pre-computation
    val location = sc.textFile("hdfs://linux1:9000/user/hadoop/xy2.txt")
    val sbxx = sc.textFile("hdfs://linux1:9000/user/hadoop/sbxx.txt")
    val sbbh_loc = location.map { i =>
        i.split("\t")
      }.map{  s=>
        val k = s(0)
        val v1 = s(1).toDouble
        val v2 = s(2).toDouble
        (k,v1,v2)
      }
      val sbxx_kv=sbxx.map(i=>
        i.split("\t")
      ).map{
        s=>(s(0),s(1))
      }
      val tmp = sbbh_loc.collect()
      val map = Map.empty[(String,String), Double]
      for(s1<-tmp){
        for(s2<-tmp){
          val k =(s1._1,s2._1)
          val v = dis(s1._2,s1._3,s2._2,s2._3)
          map(k)=v
        }
      }
      val col_sbxx_kv = sbxx_kv.collect
      //sbbh->sbmc
      val sbxx_map =Map.empty[String, String]
      for(kv<-col_sbxx_kv){
        sbxx_map(kv._1)=kv._2
      }
      def compute_speed(r1:Record , r2:Record):Double={
        val time = time_diff(r1.sj,r2.sj)
        //avoid null exception
        val length = map.getOrElse((r1.sbbh,r2.sbbh),0.0)
        val v = (length*1000)/time
        v
      }
      //Data Extraction
      //get Info per Minute
      val conn = new JdbcConn
      val list = conn.getHphm(args(0),args(1))
      //avoid null exception
      if (list ==null) throw new Exception
      val  gcjl = sc.parallelize(list,2).cache()
      //卡口流量分析
      val gcjl1 = gcjl.filter{
        r => if (r.CLSD=="0") false
        else true
      }
      val df =gcjl1.toDF().repartition(10)
      val func = (str1: String, s: String) => {
        //2014-09-01 00:00
        val ss = str1
        val s1 = s.replace(":", "-")
        val s2 = s1.replace(" ", "-")
        val s3 = s2.substring(0, 16)
        str1+"-"+s3
      }
      sqlContext.udf.register("ChgTime", func)

      df.registerTempTable("gcjl")

      val df1 = sqlContext.sql("select ChgTime(SBBH,JGSJ) AS KEY,CLSD,HPHM from gcjl ")
      df1.registerTempTable("gcjl1")
      val df2 = sqlContext.sql("select KEY,AVG(CLSD),COUNT(HPHM) FROM gcjl1 group by KEY")
      df2.foreachPartition{
        partition =>
          val conf = HBaseConfiguration.create()
          conf.set("hbase.zookeeper.quorum", "linux1")
          val table = new HTable(conf, Bytes.toBytes("kksd"))
          table.setWriteBufferSize(1024*1024*10)
          partition.foreach{
            kksd =>
              val put= new Put(Bytes.toBytes(kksd.getString(0)))
              val value = kksd.getDouble(1).toString+"-"+kksd.getLong(2).toString
              put.add(Bytes.toBytes("F1"), Bytes.toBytes("value"), Bytes.toBytes(value))
              table.put(put)
          }
          table.close
      }
      //套牌
      val ExtInfo = gcjl.filter{
        r => if (r.HPHM=="00000000" || r.HPHM=="null") false
          else true
      }
      println(ExtInfo.count+" "+map.size)
      //ExtInfo.collect().foreach(println)
      //compare records

      ExtInfo.foreachPartition {
        partition =>
          //create HBase connection
          val conf = HBaseConfiguration.create()
          conf.set("hbase.zookeeper.quorum", "linux1")
          val table = new HTable(conf, Bytes.toBytes("record"))
          val table1 = new HTable(conf, Bytes.toBytes("warning"))
          val table2 = new HTable(conf, Bytes.toBytes("company"))
          //10Mb buffer
          table.setWriteBufferSize(1024*1024*10)
          table1.setWriteBufferSize(1024*1024*10)
          table2.setWriteBufferSize(1024*1024*10)
          //to avoid Serializable def a function at local
          //HTable!Serializable
          def PutRecordIntoHbase( rowkey:String ,value:String,table:HTable ):Unit ={
            val put= new Put(Bytes.toBytes(rowkey))
            put.add(Bytes.toBytes("F1"), Bytes.toBytes("value"), Bytes.toBytes(value))
            table.put(put)
          }
          import scala.collection.JavaConversions._

          partition.foreach{
            info =>
              //套牌分析
              val get= new Get(Bytes.toBytes(info.HPHM))
              get.addColumn(Bytes.toBytes("F1"), Bytes.toBytes("value"))
              val result = table.get(get)
              if (!result.isEmpty) {

                var v:String = new String()
                for (kv <- result.list) {
                  v = Bytes.toString(kv.getValue)
                }
                val ss= v.split("\\|")
                val last_Rec = Record(ss(1),ss(2))
                //compare the possibility
                val speed = compute_speed(rec2Rec(info),last_Rec)
                if ((speed>30) && (map.getOrElse((info.SBBH,last_Rec.sbbh),0.0)>3.0) ){
                  //write warning msg into hbase
                  val now = new Date()
                  val df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                  val NowStr = df1.format(now)
                  val rowkey = NowStr +"-"+info.HPHM
                  val value = last_Rec.sj+"|"+last_Rec.sbbh+"|" +sbxx_map(last_Rec.sbbh)+"|"+
                    info.JGSJ+"|"+info.SBBH+"|"+sbxx_map(info.SBBH)+"|"+
                    speed.toString
                  PutRecordIntoHbase(rowkey,value,table1)
                }
                //put into hbase anyway
                val rowkey = info.HPHM
                //generate a string linked with HPHM-JGSJ-SBBH
                val value = info.HPHM+"|"+info.JGSJ+"|"+info.SBBH
                //for taopai
                PutRecordIntoHbase(rowkey,value,table)
                //for 伴随
                PutRecordIntoHbase(info.SBBH+"-"+info.CDFX+"-"+info.JGSJ.substring(0,16),info.HPHM,table2)
              }
              else{
                val rowkey = info.HPHM
                //generate a string linked with HPHM-JGSJ-SBBH
                val value = info.HPHM+"|"+info.JGSJ+"|"+info.SBBH
                //for  套牌
                PutRecordIntoHbase(rowkey,value,table)
                //for 伴随
                PutRecordIntoHbase(info.SBBH+"-"+info.CDFX+"-"+info.JGSJ.substring(0,16),info.HPHM,table2)
              }
          }
          table.close
          table1.close
          table2.close
      }
      println("over!")
      sc.stop()
  }
}

