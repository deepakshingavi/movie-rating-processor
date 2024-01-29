package com.dp.training.model

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Schema {

  val user_data_schema = new StructType(Array(
    StructField("UserID", StringType, true),
    StructField("Gender", StringType, true),
    StructField("Age", StringType, true),
    StructField("Occupation", StringType, true),
    StructField("ZipCode", StringType, true)
  ))





}
