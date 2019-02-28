package com.imooc.spark.project


import com.imooc.spark.project.dao.{CourseClickCountDAO, CourseSearchClickCountDAO}
import com.imooc.spark.project.domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import com.imooc.spark.project.utils.DateUtils
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.mutable.ListBuffer
/**
  * 使用Spark Streaming处理Kafka数据
  */

object ImoocStatStreamingApp {

  def main(args: Array[String]): Unit = {

    if(args.length != 3){
      println("Usage: ImoocStatStreamingApp <bootstrap.server>, <group> <topics> ")
      System.exit(1)
    }
    val Array(bootstrap, groupId, topics) = args

    val sparkConf = new SparkConf().setAppName("ImoocStatStreamingApp") //.setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(15))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> bootstrap,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "group.id" -> groupId
    )

    val topicsList = topics.split(",")


//    val topicMap = topics.split("，").map((_, numThreads.toInt)).toMap

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicsList, kafkaParams)
    )

//    messages.map(record => record.value).count().print

  //测试步骤二：数据清洗
    val logs = messages.map(record => record.value)
    val cleanData = logs.map(line => {
      val infos = line.split("\t")
//      infos(2) = "GET /class/131.html HTTP/1.1"
      val url = infos(2).split(" ")(1)
      var courseId = 0
//      把实战课程的编号拿到了
      if(url.startsWith("/class")) {
        val courseIdHTML = url.split("/")(2)
        courseId = courseIdHTML.substring(0, courseIdHTML.lastIndexOf(".")).toInt
      }

      ClickLog(infos(0), DateUtils.parseToMinute(infos(1)), courseId, infos(3).toInt, infos(4))
    }).filter(clicklog => clicklog.courseId != 0)
//    cleanData.print()

    //测试步骤三：统计到现在为止实战课程访问量

    cleanData.map(x => {
      //HBase rowkey设计： 20171111_88
      (x.time.substring(0, 8) + "_" + x.courseId, 1)
    }).reduceByKey(_ + _).foreachRDD(rdd =>{
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[CourseClickCount]
        partitionRecords.foreach(pair => {
          list.append(CourseClickCount(pair._1, pair._2))
        })

        CourseClickCountDAO.save(list)
      })
    })
    //测试步骤四：统计从搜索引擎过来的到现在为止实战课程访问量
    cleanData.map(x => {
      val referer = x.referer.replaceAll("//", "/")
      val splits = referer.split("/")

      var host = ""
      if (splits.length > 2){
        host = splits(1)
      }
      (host, x.courseId, x.time)
    }).filter(_._1 != "").map(x => {
      (x._3.substring(0, 8) + "_" + x._1 + x._2, 1)
    }).reduceByKey(_ + _).foreachRDD(rdd =>{
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[CourseSearchClickCount]
        partitionRecords.foreach(pair => {
          list.append(CourseSearchClickCount(pair._1, pair._2))
        })

        CourseSearchClickCountDAO.save(list)
      })
    })


    ssc.start()
    ssc.awaitTermination()
  }

}
