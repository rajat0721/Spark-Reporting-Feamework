package lumiq.ai.transformer.util
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Constants {

  val s3TransformedBucketName = "s3://clix-lumiq-edl/"
  val transformedPath = "transformed/"
  val reportName = "GLoveral"  // pass as argument
  val indexedTablePath = "indexed/platinum/credentek/CLIX_UAT/"
  val RC_RECONCILED_TXN = "RC_RECONCILED_TXN.parquet"
  val RC_LEG_TXN = "RC_LEG_TXN.parquet"
  val athenaDiamondDBName = "LUMIQ_EDL_DIAMOND_TABLES"

}
