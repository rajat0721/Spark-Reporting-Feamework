package lumiq.ai.transformer.util

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object common {

  def Trans_init(spark:SparkSession)={
    get_mrCustTbl(spark)
    //different tables could be initialised with views from index
  }

  def get_mrCustTbl(spark:SparkSession)={
    val mr_customer_tbl=getDataFrameWithLatestRecords(spark,"",S3.ParqRead(spark,""),"")
      .select("CONT_NO", "IS_MASTER_POLICY_CONCEPT").persist()
    mr_customer_tbl.createOrReplaceTempView("mr_customer_tbl")
  }

  def getDataFrameWithLatestRecords(spark: SparkSession, table: String, dataFrame: DataFrame, schemaName: String): DataFrame = {

    val primaryKeyColumns = spark.read.option("multiLine", value = true).json("s3://max-lumiq-edl-uat/misc/primarycolumnlist.json")
    //primaryKeyColumns.createOrReplaceTempView("JsonView")

    //val w = Window.partitionBy("a")
    var Pc  = List[String]()
    var Tn = ""

    primaryKeyColumns.foreach(r=>{
      Pc = r.getAs[String](r.fieldIndex("primaryColumn")).split(",").toList :+ "upd_date"
      Tn = r.getAs(r.fieldIndex("tablename"))
      val w = Window.partitionBy(Pc.head,Pc.tail:_*)
      val Df = S3.ParqRead(spark,"s3://max-lumiq-edl-uat/indexed/maximusp/CSR/"+Tn+".parquet")
        .withColumn("max_upd_date",max("upd_date").over(w))
        .filter(col("upd_date")===col("max_upd_date")).persist()
      Df.createOrReplaceTempView(Tn)
    }
    )
    spark.sql("")

    }

  def GetDF(spark:SparkSession,TableName:String):DataFrame={
    val disDF = spark.read.option("multiLine", value = true).json("s3://max-lumiq-edl-uat/misc/BenifitTable.json")
    val PrimaryColumn = disDF.filter(col("tablename")===TableName.toLowerCase).select("primaryColumn").collect.map(_.getString(0)).mkString(",").split(",").toList
    println("Getting Table  :  " + TableName)

    val w = Window.partitionBy(PrimaryColumn.head,PrimaryColumn.tail:_*)
    val ReturnDF = S3.ParqRead(spark,s"s3://max-lumiq-edl-uat/indexed/maximusp/CSR/${TableName.toUpperCase}.parquet")
      .withColumn("max_upd_date",max("upd_date").over(w))
      .filter(col("upd_date")===col("max_upd_date"))

    ReturnDF
  }



}
