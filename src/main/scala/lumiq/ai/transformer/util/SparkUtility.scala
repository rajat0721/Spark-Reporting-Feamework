package lumiq.ai.transformer.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkUtility {

  def getSparkSession(AppName:String): SparkSession ={
    SparkSession.builder()
      //.master("")
      .appName(AppName)
      .enableHiveSupport()
      .getOrCreate()
  }

  def init(AppName:String):SparkSession = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    getSparkSession(AppName)
  }
}
