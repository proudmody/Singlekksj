import java.text.SimpleDateFormat
import java.util.{Date, Timer, TimerTask}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.deploy.SparkSubmit
import scala.math._

object Submit {
  def main(args: Array[String]) {
    //定时任务
    val timer = new Timer()
    timer.schedule(new MyTask(),0,20000)
  }
  var flag = true
  var lastMinute = 0
  class MyTask() extends TimerTask with Serializable {
    @Override
    def run: Unit = {
      val conn = new JdbcConn
      val date = conn.getDate()
      val df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      var now: Date = new Date()
      if (date == null) return
      else now = df1.parse(date)
      now.setSeconds(0)
      println(date+" "+ flag + " " + lastMinute + " " + now.getMinutes)
      //每分钟提交一次SparkApp
      if ((flag && (lastMinute == 0)) || (abs(now.getMinutes - lastMinute) >= 1)) {
        val endMinute = now.getMinutes
        now.setMinutes(endMinute)
        val end = df1.format(now)
        def getlast(f:Boolean,a:Int,b:Int):Int ={
          if (f) a
          else b
        }
        val begMunute = getlast((flag && (lastMinute == 0)),endMinute - 1,lastMinute)
        now.setMinutes(begMunute)
        val begin = df1.format(now)
        println(begin+" "+end)
        if (!submit(begin, end)) return
        lastMinute = endMinute
        flag = false
      }
    }

    def submit(begin: String, end: String):Boolean= {
      val args = Array(
        "--master",
        "spark://linux1:70777",
        "--class",
        "InfoExtract",
        "D:\\spark\\web\\Singlekksj\\out\\artifacts\\Singlekksj_jar\\Singlekksj.jar",
        begin,
        end
      )
      //val conf: Configuration = new Configuration
      //HDFSUtil.UploadLocalFileHDFS(conf, "D:\\spark\\web\\Singlekksj\\out\\artifacts\\Singlekksj_jar\\Singlekksj.jar", "/jar/Singlekksj.jar")
      System.setProperty("HADOOP_USER_NAME", "hadoop")
      try {
        SparkSubmit.main(args)
      }
      catch {
        case e:Exception => e.printStackTrace();return  false;
      }
      true
    }
  }
}



