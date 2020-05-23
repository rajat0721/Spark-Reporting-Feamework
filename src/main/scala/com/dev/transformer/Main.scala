package com.dev.transformer

import com.dev.transformer.reports.{Report1, Report2}
import com.dev.transformer.util.SparkUtility

object Main {

  def main(args: Array[String]): Unit = {


    if (args.length < 2) {
      println("/***\n" +
        "JobName = args(0)      -- Transform \n"
        + "reportName = args(1) -- report1, report2, ALL | NA \n"
      +"***/")

      sys.exit(1)
    }
    val JobName = args(0)
    val reportName = args(1)
    val spark = SparkUtility.init(JobName)

    reportName match {
      case "report1" => println("Running Job for " + reportName)
        Report1.report1Creator(spark, reportName)
      case "report2" => println("Running Job for " + reportName)
        Report2.report2Creator(spark, reportName)
      case "NA" | "ALL" => {
        println("Running for all reports")
        Report1.report1Creator(spark)
        Report2.report2Creator(spark)
      }
    }
  }

}
