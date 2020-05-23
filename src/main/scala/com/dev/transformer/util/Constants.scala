package com.dev.transformer.util

object Constants {

  val s3TransformedBucketName = "s3://edl/"
  val transformedPath = "transformed/"
  val reportName = "GLoveral"  // pass as argument
  val indexedTablePath = "indexed/platinum/credentek/CLIX_UAT/"
  val RC_RECONCILED_TXN = "TABLE1.parquet"
  val RC_LEG_TXN = "TABLE2.parquet"
  val athenaDiamondDBName = "EDL_DIAMOND_TABLES"
}
