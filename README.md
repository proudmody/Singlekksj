# Singlekksj
Fake plate Detection &amp; Trafic flow statistics,Using Spark
##实现的功能：
* 比对卡口数据中的车辆信息，基于“一辆车不可能较短时间内出现在两个地点”的原理，找出并展示可能涉嫌套牌的车辆；
* ps：卡口数据包含车牌信息，地理信息，时间。
* 统计一个时间段内过往的车流量信息，为另一个流量分析与预测的系统提供数据。

##关键算法：
* 由于交通道路的形大多呈“井”字形，可以通过直接对两个设备之间的经度差与纬度差求和，
再计算球面距离的方式，近似地计算两个卡口之间的距离，即车辆的行驶距离。
* 详见Singlekksj/src/InfoExtract.scala中的dis函数。

##主要技术
主要使用了`SparkSql` 和`SparkCore`库
##关于
* `DBlink信息删去了`
* `没有数据`，这个项目就是拿来看看吧。
