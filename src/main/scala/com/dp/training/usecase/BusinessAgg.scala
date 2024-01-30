package com.dp.training.usecase

import com.dp.training.udf.DataQualityUDF
import org.apache.spark.sql.{DataFrame, Encoder, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object BusinessAgg {

  def bucketByAndSave(inputDF: DataFrame, bucketByKey : String,
                      outputFilePath: String,
                      tableName:String,noOfBuckets : Int = 10 ): Unit = {
    inputDF
      .write
      .format("parquet")
      .option("compression", "snappy")
      .mode(SaveMode.Overwrite)
      .bucketBy(noOfBuckets, bucketByKey)
      .option("path", outputFilePath+tableName)
      .saveAsTable(tableName)
  }

  def writeMovieData(inputDF: DataFrame, outputFilePath: String,tableName:String): Unit = {
    bucketByAndSave(inputDF,"MovieID",outputFilePath,tableName)
  }

  def writeUserData(inputDF: DataFrame, outputFilePath: String,
                    tableName:String ): Unit = {
    bucketByAndSave(inputDF,"UserID",outputFilePath,tableName)
  }

  /**
   * This code fails to write when rating dir already exists
   * Note :
   * @param inputDF
   * @param outputFilePath
   * @param tableName
   */
  def writeRatingData(inputDF: DataFrame, outputFilePath: String,
                    tableName:String ): Unit = {
    inputDF
      .withColumn("Year", year(to_date(from_unixtime(col("Timestamp")))))
      .write
      .option("compression", "snappy")
      .mode(SaveMode.Overwrite)
      .partitionBy("Year")
      .parquet(outputFilePath+tableName)
  }

  def writeTopRatedMovieByUser(inputDF: DataFrame, outputFilePath: String,tableName:String ): Unit = {
    bucketByAndSave(inputDF,"UserID",outputFilePath,tableName)
  }

  def writeRatingAggDataByMovie(inputDF: DataFrame, outputFilePath: String,tableName:String ): Unit = {
    bucketByAndSave(inputDF,"MovieID",outputFilePath,tableName)
  }


  def ratingAgg(inputDataFrame : DataFrame) : DataFrame = {
    inputDataFrame.groupBy("MovieID").agg(
          max("Rating").alias("MaxRating"),
            min("Rating").alias("MinRating"),
        avg("Rating").alias("AvgRating")
        )
  }


  def getLatestTop3RateMoviesForUser(inputDataFrame : DataFrame) : DataFrame = {
    val partitionByUserOrderByRating = Window.partitionBy("UserID").orderBy(desc("Rating"),desc("Timestamp"))


    val topRatedMovieForThatUser = inputDataFrame
      .withColumn("RowNum", row_number().over(partitionByUserOrderByRating))
      .filter("RowNum <= 3").drop("RowNum")

    topRatedMovieForThatUser
  }

  def loadData[T](dataFilePth: String)(implicit encoder: Encoder[T],spark: SparkSession): DataFrame ={


    val schema = implicitly[Encoder[T]].schema
    val dataFrame = spark.read
      .option("header", "false")
      .option("delimiter","::")
      .schema(schema)
      .csv(dataFilePth)

    dataFrame

  }

  def loadMovieData[T](dataFilePth: String)(implicit encoder: Encoder[T],spark: SparkSession): DataFrame ={

    val strToArrUDF = DataQualityUDF.mapStrToArr()
    val df = BusinessAgg.loadData[T](dataFilePth)
    df.withColumn("Genres",strToArrUDF(col("Genres")))

  }
}
