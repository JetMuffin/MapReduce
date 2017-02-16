package edu.nju.jetmuffin.cmd

import java.io.FileInputStream
import java.util.Properties

/**
  * Created by cj on 2017/2/15.
  */

/**
  * Load properties file that can be later accessed as values of Conf instance
  */
class Conf() {

  // Load properties
  val properties = new Properties()
  val path = Thread.currentThread().getContextClassLoader.getResource("config.properties").getPath
  properties.load(new FileInputStream(path))

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
