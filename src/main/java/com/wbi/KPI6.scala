package com.wbi
//6. Youngest Country - Yearly distribution of youngest Countries
import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import java.lang.Long

object KPI6 {
  def main(args: Array[String]) {

  }
  val spark = SparkSession.builder().appName("KPI_1").master("local").getOrCreate()
  val data = spark.read.csv("D:\\Mahima\\My Dev Space\\workspace\\WBI_Scala\\HP_And_SK\\World Bank\\World_Bank_Indicators.csv").rdd

  var KPI6 = {
    //youngest Country = (83,Hong Kong SAR, China)
    var result = data.map { line =>
      {
        var youngestCountry = line.getString(14)
        var youngNum = 0L
        if (youngestCountry != null)
          youngNum = Long.parseLong(youngestCountry)
        //(count,country name)
        (youngNum, line.getString(0))
      }
    }.sortByKey(false).take(10)
    result.foreach { println }
  }
  /*var KPI7 = {
    //youngest Country = (83,Hong Kong SAR, China)
    var result = data.map { line =>
      {
        var youngestCountry = line.getString(14)
        var youngNum = 0L
        if (youngestCountry != null)
          youngNum = Long.parseLong(youngestCountry)
        (line.getString(0), youngNum)
      }
    }.groupByKey()
      .map { rec => { (rec._1, rec._2.max) } }.sortBy(rec => rec._2, false).take(10)
    result.foreach { println }
  }*/
  spark.stop()
}