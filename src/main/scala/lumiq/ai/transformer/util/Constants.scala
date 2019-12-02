package lumiq.ai.transformer.util
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Constants {

    // global variables
    val envList = List("DEV", "UAT", "PROD")
    val fileVelList = List("HV", "LV")
    val yr2000epts: Long = 946684800000L
    val pathToSaveInHDFS = "/indexingJobs"
    val pathForPKeyList = "s3://clix-lumiq-edl-artifacts/incremental-table-info/PKeyList.json"

    val customSchema = StructType(
      List(
        StructField("edl_created_at", StringType, true),
        StructField("edl_created_by", StringType, true),
        StructField("op", StringType, true),
        StructField("cdc_timestamp", StringType, true),
        StructField("cdc_row_number", StringType, true)
      )
    )

    object SupportedFileExt extends Enumeration {
      type FileExtType = Value
      val csv = Value("csv")
      val avro = Value("avro")

      def isFileExtValid(fileExt: String) = values.exists(_.toString == fileExt)
    }

    // CONSTANT FUNCTIONS
    def genAthenaDBName(env: String): (String, String) = {

      val athenaPlatinumDBName: String = s"LUMIQ_EDL_${env}_PLATINUM_TABLES"
      val athenaHistoryDBName: String = s"LUMIQ_EDL_${env}_HISTORY_TABLES"

      (athenaPlatinumDBName, athenaHistoryDBName)
    }

    def determinePathInHDFS(runId: String): String = {

      (pathToSaveInHDFS + "/" + runId)
    }

    def generateS3FilePath(bucketName: String, source: String, schemaName: String, tableName: String, runId: String): (String, String, String) = {

      val s3BucketPlatinumPath = "s3://" + bucketName + "/indexed/platinum/" + source + "/" + schemaName + "/" + tableName + ".parquet"
      val s3BucketHistoryPath = "s3://" + bucketName + "/indexed/history/" + source + "/" + schemaName + "/" + tableName + ".parquet"
      val s3BucketPlatinumTempWritePath = "s3://" + bucketName + "/indexed/platinum/tmp_write/" + source + "/" + schemaName + "/" + runId + "/" + tableName + ".parquet"

      (s3BucketPlatinumPath, s3BucketHistoryPath, s3BucketPlatinumTempWritePath)
    }

}
