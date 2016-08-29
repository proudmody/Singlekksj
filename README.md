# Singlekksj
Fake plate Detection &amp; Trafic flow statistics,Using Spark
实现的功能：
比对卡口数据中的车辆信息，基于“一辆车不可能较短时间内出现在两个地点”的原理，找出并展示可能涉嫌套牌的车辆；
统计一个时间段内过往的车流量信息，为另一个流量分析与预测的系统提供数据。
关键算法：
由于交通道路的形大多呈“井”字形，可以通过直接对两个设备之间的经度差与纬度差求和，
再计算球面距离的方式，近似地计算两个卡口之间的距离，即车辆的行驶距离。
详见Singlekksj/src/InfoExtract.scala中的dis函数。
