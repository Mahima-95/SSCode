package com.wbi
//4. Highest GDP growth - List of Countries with highest GDP growth from 2009 to 2010 in descending order
import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import java.lang.Long

object KPI4 {
  def main(args: Array[String]) {

  }
  val spark = SparkSession.builder().appName("KPI_1").master("local").getOrCreate()
  val data = spark.read.csv("D:\\Mahima\\My Dev Space\\workspace\\WBI_Scala\\HP_And_SK\\World Bank\\World_Bank_Indicators.csv").rdd

  val result = data.filter(line => {
    (line.get(1).toString().contains("2009") || line.get(1).toString().contains("2010"))
  }).map(line => {
    var gdp = line.getString(17)
    var highestGdp = 0L
    if (gdp != null) {
      highestGdp = Long.parseLong(gdp)
    }
    (highestGdp, line.getString(0))
  }).sortByKey(false).take(10)
  result.foreach { println }
}