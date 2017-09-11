package com.github.plavreshin

import java.time.Instant

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ReadingWithSparkInitial {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Args must be supplied")
      sys.exit(0)
    }

    val startedAt = Instant.now
    println(s"Job Starting time: $startedAt")

    val conf = new SparkConf()
      .setAppName("ReadingWithSparkAdvanced")
      .set("spark.driver.maxResultSize", "1g")

    val sparkSession = SparkSession.builder.config(conf).getOrCreate()

    val csv = sparkSession
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(args(0))

    csv.show()

    println(s"Execution took: ${ Instant.now.toEpochMilli - startedAt.toEpochMilli }ms")
    sparkSession.stop()
  }
}
