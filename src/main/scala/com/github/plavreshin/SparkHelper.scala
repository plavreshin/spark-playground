package com.github.plavreshin

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

object SparkHelper {
  def getOrCreateSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("ReadingWithSparkAdvanced")
      .set("spark.driver.maxResultSize", "1g")
    SparkSession.builder.config(conf).getOrCreate()
  }

  def csvFrame(session: SparkSession, fileName: String): Try[DataFrame] = {
    Try(session
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(fileName))
  }
}
