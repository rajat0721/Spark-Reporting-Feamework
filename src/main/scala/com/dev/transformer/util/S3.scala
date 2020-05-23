package com.dev.transformer.util

import org.apache.spark.sql.{DataFrame, SparkSession}

object S3 {

  def ParqRead(spark:SparkSession,path:String)={
    spark.read.parquet(path)
  }

  def ParqWrite(spark:SparkSession, FinalDF:DataFrame, path:String, WriteMode:String="overwrite"){
    FinalDF.coalesce(2).write.mode(WriteMode).parquet(path)
  }
}
