package lumiq.ai.transformer

import lumiq.ai.transformer.jobs._
import lumiq.ai.transformer.util.SparkUtility


object Main {

  def main(args: Array[String]): Unit = {


    if (args.length < 2) {
      println("/***\n" +
        "JobName = args(0)      -- Transform \n"
        + "reportName = args(1) -- GLoveral, bankOveral, ALL | NA \n"
      +"***/")

      sys.exit(1)
    }
    val JobName = args(0)
    val reportName = args(1)
    val spark = SparkUtility.init(JobName)

    reportName match {
      case "GLoveral" => println("Running Job for " + reportName)
        GLOveral.GL_OverallCreator(spark, reportName)
      case "bankOveral" => println("Running Job for " + reportName)
        bankOveral.bankOveralCreator(spark, reportName)
      case "NA" | "ALL" => {
        println("Running for all reports")
        GLOveral.GL_OverallCreator(spark)
        bankOveral.bankOveralCreator(spark)
      }
    }
  }

}
