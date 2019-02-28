package com.imooc.spark.project.domain


/**
  * 实战课程点击数
  * @param day_course  对应rowkey，20171111_1
  * @param click_count  对应访问总数
  */
case class CourseClickCount (day_course:String, click_count:Long)
