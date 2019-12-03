package lumiq.ai.transformer.jobs

import lumiq.ai.transformer.util.{Constants, S3, hiveUtility}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object bankOveral {

  def bankOveralCreator(spark:SparkSession,reportName:String="bankOveral"): Unit ={
    import spark.implicits._

    val wfunc = Window.orderBy(asc("RECON_SEQ_NO"))


    val rc_reconciled_txn = S3.ParqRead(spark,Constants.s3TransformedBucketName + Constants.indexedTablePath + Constants.RC_RECONCILED_TXN)
      .select($"TERMINAL_ID".as("bank_acc_code"), $"RECON_STATUS", $"REMARKS".as("Knock_Off_Reference"),$"RECON_SEQ_NO")

    val rc_leg_txn = S3.ParqRead(spark,Constants.s3TransformedBucketName + Constants.indexedTablePath + Constants.RC_LEG_TXN)
      .select($"REF1", $"REF2", $"DATE_1".as("value_date"), $"DATE_2".as("transaction_date"), $"CHAR_1".as("cr_dr"),
    $"AMT_1".as("amount"), $"REF3".as("description"), lit(null).cast("string").as("Remarks"),
        lit(null).cast("string").as("ref_upload_entry"), lit(null).cast("string").as("recon_code"),$"RECON_SEQ_NO")

    val bankOveral_inter = rc_reconciled_txn.join(rc_leg_txn,Seq("RECON_SEQ_NO"),"inner").distinct()

    val bankOveral = bankOveral_inter.withColumn("S.No.",(row_number.over(wfunc)).cast("string"))

    S3.ParqWrite(spark,bankOveral,Constants.s3TransformedBucketName + Constants.transformedPath + reportName + ".parquet")

    hiveUtility.createHiveTableIfNotExists(spark,bankOveral.columns.toList,reportName,Constants.athenaDiamondDBName,Constants.s3TransformedBucketName + Constants.transformedPath + reportName + ".parquet")

  }
}
