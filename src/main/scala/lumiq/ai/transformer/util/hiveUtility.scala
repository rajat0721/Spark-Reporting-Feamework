package lumiq.ai.transformer.util

import org.apache.spark.sql.SparkSession

object hiveUtility {

  def createHiveTableIfNotExists(spark: SparkSession, columnList: List[String], tableName: String, athenaDBName: String, s3DestinationPath: String) {

    var schema = ""
    columnList.foreach { columnName => {
      if (!(columnName == "edl_created_at")) {
        schema += "`" + columnName + "`"
        schema += " STRING,"
      }
    }
    }
    schema = schema.dropRight(1)

    spark.sql(s"CREATE DATABASE IF NOT EXISTS `${athenaDBName}`")

    spark.sql(
      s"""CREATE EXTERNAL TABLE  IF NOT EXISTS `${athenaDBName}`.`${tableName}` (${schema}) stored as parquet LOCATION '${s3DestinationPath}'""".stripMargin
    )
  }

  def updatePartitionsInHiveTbl(spark: SparkSession, athenaDBName: String, tableName: String, env: String) = {

    spark.sql("MSCK REPAIR TABLE " + athenaDBName + "." + tableName)
  }

}
