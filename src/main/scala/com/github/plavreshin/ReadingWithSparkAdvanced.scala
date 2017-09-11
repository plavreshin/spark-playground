package com.github.plavreshin

import java.time.Instant

import scala.util.control.NonFatal

object ReadingWithSparkAdvanced {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Args must be supplied")
      sys.exit(0)
    }
    val startedAt = Instant.now
    println(s"Job Starting time: $startedAt")

    val session = SparkHelper.getOrCreateSession
    (for {
      csv <- SparkHelper.csvFrame(session, args(0))
    } yield {
      csv.show()
    }).recover {
      case NonFatal(ex) =>
        println(s"Failed to proceed with csv file with exception: $ex")
        session.stop()
    }

    println(s"Execution took: ${ Instant.now.toEpochMilli - startedAt.toEpochMilli }ms")
  }
}
