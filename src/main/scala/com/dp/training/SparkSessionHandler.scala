package com.dp.training

import org.apache.spark.sql.SparkSession

object SparkSessionHandler {


  var spark :SparkSession = _

  /**
   * Start Spark context with default driver and executor memory
   *
   * @param sparkMaster
   * @return
   */
  def initSparkSession(sparkMaster: String,jobName : String  = "Movie Rating Processor"): SparkSession = {
    spark = SparkSession
      .builder()
      .master(sparkMaster)
      .appName(jobName)
      .config("spark.driver.memory", "12g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()
    spark
  }

  def shutdownSC(): Unit = {
    spark.stop()
  }
}
