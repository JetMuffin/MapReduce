package edu.nju.jetmuffin.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by cj on 2017/2/15.
  */

/**
  * Settings of spark environment
  */
object SparkEnv {

  Logger.getLogger("org.apache.spark").setLevel(Level.INFO)

  val conf = new SparkConf().setMaster("local").setAppName("RecommendSystem")
  val sc = new SparkContext(conf)
}
