package edu.nju.jetmuffin.cmd

import java.io.FileInputStream
import java.util.Properties

/**
  * Created by cj on 2017/2/15.
  */

/**
  * Load properties file that can be later accessed as values of Conf instance
  */
class Conf(path: Option[String]) {

  // Load properties
  val properties = new Properties()
  path match {
    case Some(str) => str
    case None => Thread.currentThread().getContextClassLoader.getResource("config.properties").getPath
  }
  properties.load(new FileInputStream(path.get))

  def get(key: String, defaultValue: String): String = {
    return properties.getProperty(key, defaultValue)
  }

  def getInt(key: String, defaultValue: Int): Int = {
    val value = properties.getProperty(key)
    return if(value == null) defaultValue else value.toInt
  }

  def getDouble(key: String, defaultValue: Double): Double = {
    val value = properties.getProperty(key)
    return if(value == null) defaultValue else value.toDouble
  }
}
