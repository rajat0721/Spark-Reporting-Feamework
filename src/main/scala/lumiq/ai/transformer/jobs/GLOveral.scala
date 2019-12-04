package lumiq.ai.transformer.jobs
import lumiq.ai.transformer.util.{Constants, S3, hiveUtility}
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, _}
import org.apache.spark.sql.expressions.Window


object GLOveral {

  def GL_OverallCreator(spark:SparkSession, reportName:String="GLoveral"){

    import spark.implicits._

    val wfunc = Window.orderBy(asc("RECON_SEQ_NO"))

    val rc_reconciled_txn = S3.ParqRead(spark,Constants.s3TransformedBucketName + Constants.indexedTablePath + Constants.RC_RECONCILED_TXN)
      .select($"TERMINAL_ID".as("Bank_Account_Code"), $"RECON_STATUS".as("Status"), $"RECON_SEQ_NO",$"LEG1_REF1")

    val rc_leg_txn = S3.ParqRead(spark,Constants.s3TransformedBucketName + Constants.indexedTablePath + Constants.RC_LEG_TXN)
      .select($"DATE_1".as("Default_Effective_Date"),$"AMT_1".cast("double").cast("string").as("Entered_Dr"),
        $"AMT_2".cast("double").cast("string").as("Entered_Cr"),$"AMT_3".cast("double").cast("string").as("Entered_Net")
        , $"REF1".as("Loan_Account_Number"),$"REF3".as("Actual_Line_Description"),
        $"FILE_FORMAT_CD".as("Sub_System_Type"), $"DATE_2".as("Posted_Date"),$"TEXT_3".as("Batch_Name"),
        $"TEXT_4".as("Journal_Name"),$"TEXT_5".as("User_Je_Category_Name"), $"TEXT_6".as("User_Je_Source_Name_GL"),
        $"DATE_4".as("Cheque_Date"),$"BUSINESS_DT".as("Value_Date"),$"LONG_TEXT_4".as("Je_Line_Number"),
        $"REF2".as("Invoice_Number"),$"TEXT_1".as("Currency_Code"),$"TEXT_2".as("Legal_Entity"), $"TEXT_9".as("Business"),
        $"TEXT_10".as("Natural_Account"),$"TEXT_7".as("Product"),$"TEXT_8".as("Branch"),$"LONG_TEXT_1".as("Function"),
    $"LONG_TEXT_2".as("Intercompany"),$"RECON_SEQ_NO")

    val GL_overall_inter = rc_reconciled_txn.join(rc_leg_txn,Seq("RECON_SEQ_NO"),"inner").distinct()

    val GLoveral= GL_overall_inter.withColumn("S_No",(row_number.over(wfunc)).cast("string"))
        .select($"S_No",$"Bank_Account_Code",$"Status",$"RECON_SEQ_NO",$"LEG1_REF1",lit(null).cast("string").as("internal_remarks")
        ,lit(null).cast("string").as("manual_reference"),$"LEG1_REF1".as("System_Reference"),$"Default_Effective_Date"
        ,$"Entered_Dr",$"Entered_Cr",$"Entered_Net",$"Loan_Account_Number",$"Actual_Line_Description",$"Sub_System_Type",$"Posted_Date",$"Batch_Name"
        ,$"Journal_Name",$"User_Je_Category_Name",$"User_Je_Source_Name_GL",$"Cheque_Date",lit(null).cast("string").as("virtual_account_number")
          ,lit(null).cast("string").as("customer_name"),$"Value_Date",$"Je_Line_Number",$"Invoice_Number",$"Currency_Code",$"Legal_Entity"
          ,$"Business",$"Natural_Account",$"Product",$"Branch",$"Function",$"Intercompany")

    S3.ParqWrite(spark,GLoveral,Constants.s3TransformedBucketName + Constants.transformedPath + reportName + ".parquet")

    hiveUtility.createHiveTableIfNotExists(spark,GLoveral.columns.toList,reportName,Constants.athenaDiamondDBName,Constants.s3TransformedBucketName + Constants.transformedPath + reportName + ".parquet")
  }
}
