package com.imooc.spark.project.domain


/**
  * 清洗后的日志信息
  * @param ip 日志ip地址
  * @param time 日志访问时间
  * @param courseId 课程id
  * @param statusCode 日志访问状态码
  * @param referer  日志访问referer
  */
case class ClickLog(ip:String, time:String, courseId:Int, statusCode:Int, referer:String)
