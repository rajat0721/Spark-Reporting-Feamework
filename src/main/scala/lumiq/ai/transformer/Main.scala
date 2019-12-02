package lumiq.ai.transformer

import lumiq.ai.transformer.jobs._
import lumiq.ai.transformer.util.SparkUtility


object Main {
  def main(args: Array[String]): Unit = {

    val JobName = args(0)
    val reportName = args(1)

    val spark = SparkUtility.init(JobName)

    reportName match {
      case "GLOveral"      => GLOveral.GL_OverallCreator(spark)
      case "bankOveral"    => bankOveral.bankOveralCreator(spark)
      case "NA"|"ALL"      => {
        GLOveral.GL_OverallCreator(spark)
        bankOveral.bankOveralCreator(spark)
      }
    }
  }
}
