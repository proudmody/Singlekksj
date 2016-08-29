import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import scala.collection.mutable.ArrayBuffer

class JdbcConn{
  def getDate():String={
    var  con:Connection= null;// 创建一个数据库连接
    var  pre:PreparedStatement = null;// 创建预编译语句对象，一般都是用这个而不用Statement
    var  result:ResultSet= null;// 创建一个结果集对象
    try
    {
      Class.forName("oracle.jdbc.driver.OracleDriver");// 加载Oracle驱动程序
      System.out.println("开始尝试连接数据库！")
      val url = "dbc:oracle:thin:@15.136.18.30:1521:snapall";// 127.0.0.1是本机地址，XE是精简版Oracle的默认数据库名
      val user = "dsf";// 用户名,系统默认的账户名
      val password = "dsf";// 你安装时选设置的密码
      con = DriverManager.getConnection(url, user, password);// 获取连接
      System.out.println("连接成功！")
      val sql = "select sysdate from dual";// 预编译语句，“？”代表参数

      val stmt = con.createStatement()
      result = stmt.executeQuery(sql);// 执行查询，注意括号中不需要再加参数
      var s:String = new String()
      while (result.next()){
        // 当结果集不为空时
         s=result.getString("sysdate")
      }
      s
    }
    catch {
      case e:Exception => e.printStackTrace(); null
    }
    finally
    {
      try
      {
        // 逐一将上面的几个对象关闭，因为不关闭的话会影响性能、并且占用资源
        // 注意关闭的顺序，最后使用的最先关闭
        if (result != null)
          result.close();
        if (pre != null)
          pre.close();
        if (con != null)
          con.close();
        System.out.println("数据库连接已关闭！");
      }
      catch {
        case e:Exception => e.printStackTrace();
      }
    }
  }

  def getHphm(begin:String,end:String):ArrayBuffer[record]={
    var  con:Connection= null;// 创建一个数据库连接
    var  pre:PreparedStatement = null;// 创建预编译语句对象，一般都是用这个而不用Statement
    var  result:ResultSet= null;// 创建一个结果集对象
    try
    {
      Class.forName("oracle.jdbc.driver.OracleDriver");// 加载Oracle驱动程序
      System.out.println("开始尝试连接数据库！")
      val url = "dbc:oracle:thin:@15.136.18.30:1521:snapall";// 127.0.0.1是本机地址，XE是精简版Oracle的默认数据库名
      val user = "dsf";// 用户名,系统默认的账户名
      val password = "dsf";// 你安装时选设置的密码
      con = DriverManager.getConnection(url, user, password);// 获取连接
      System.out.println("连接成功！")
      val sql = "select HPHM,JGSJ,CDFX,SBBH,CDBH,CLSD from sjkk_gcjl where "+
        "RKSJ<to_date('"+end+"','yyyy-mm-dd hh24:mi:ss') "+
        "AND RKSJ>=to_date('"+begin+"','yyyy-mm-dd hh24:mi:ss')";

      val stmt = con.createStatement()

      result = stmt.executeQuery(sql);// 执行查询，注意括号中不需要再加参数
      val list:ArrayBuffer[record]=new ArrayBuffer[record]
      while (result.next()){
        // 当结果集不为空时
        val tmp =record(result.getString("HPHM"),result.getString("JGSJ"),
          result.getString("CDFX"),result.getString("SBBH"),result.getString("CDBH"),result.getString("CLSD"))
        list.append(tmp)
       // System.out.println(result.getString("HPHM"))
      }
      list
    }
    catch {
      case e:Exception => e.printStackTrace(); null
    }
    finally
    {
      try
      {
        // 逐一将上面的几个对象关闭，因为不关闭的话会影响性能、并且占用资源
        // 注意关闭的顺序，最后使用的最先关闭
        if (result != null)
          result.close();
        if (pre != null)
          pre.close();
        if (con != null)
          con.close();
        System.out.println("数据库连接已关闭！");
      }
      catch {
        case e:Exception => e.printStackTrace();
      }
    }
  }
}