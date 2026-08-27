package com.example.spark.telemetry

import java.util.{HashMap => JHashMap, Map => JMap}

import org.apache.spark.SparkConf

private[telemetry] object SparkConfigAdapter {
  def toJava(conf: SparkConf): JMap[String, String] = {
    val result = new JHashMap[String, String]()
    conf.getAll.foreach { entry => result.put(entry._1, entry._2) }
    result
  }
}
