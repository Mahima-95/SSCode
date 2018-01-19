package com.wbi
//1. Highest urban population - Country having the highest urban population
import org.apache.spark.sql.SparkSession
import java.lang.Long
import java.text.SimpleDateFormat

object WBI_KPIs {
  def main(args: Array[String]) {

  }
  val spark = SparkSession.builder().appName("KPI_1").master("local").getOrCreate()
  val data = spark.read.csv("D:\\Mahima\\My Dev Space\\workspace\\WBI_Scala\\HP_And_SK\\World Bank\\World_Bank_Indicators.csv").rdd

  val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSS")
  val format2 = new SimpleDateFormat("yyyy-MM-dd")
  val format3 = new SimpleDateFormat("yyyy")

  var KPI1 = {
    val result = data.map { line =>
      {
        var urbanPopulation = line.getString(9).replaceAll(",", "")
        var urbanPopNum = 0L
        if (urbanPopulation.length() > 0)
          urbanPopNum = Long.parseLong(urbanPopulation)
        (urbanPopNum, line.getString(0))
      }
    }.sortByKey(false).first()
    print(result)
  }

  //2. Most populous Countries - List of countries in the descending order of their population
  var KPI2 = {
    val result = data.map { line =>
      {
        var population = line.getString(9).replaceAll(",", "")
        var popNum = 0L
        if (population.length() > 0) {
          popNum = Long.parseLong(population)
        }
        (line.getString(0), popNum)
      }
    }.groupByKey()
      .map { rec => { (rec._1, rec._2.max) } }.sortBy(rec => rec._2, false).take(10)
    result.foreach { println }
  }

  //3. Highest population growth - Country with highest % population growth in past decade
  var KPI3 = {
    val result = data.map { line =>
      {
        var popLess_14 = line.getString(14)
        var popBelow_65 = line.getString(15)
        var popAbove_65 = line.getString(16)
        var totalPopulation = 0L
        if (popLess_14 != null && popBelow_65 != null && popAbove_65 != null) {
          totalPopulation = Long.parseLong(popLess_14) + Long.parseLong(popBelow_65) + Long.parseLong(popAbove_65)
        }
        (totalPopulation, line.getString(0))
      }
    }.sortByKey(false).first()
    print(result._2 + " " + result._1 + " % ")
  }

  //4. Highest GDP growth - List of Countries with highest GDP growth from 2009 to 2010 in descending order
  /*  var KPI4 = {
    val result = data.map { line =>
      {
        var gdp = line.getString(17)
        var startDate = format2.parse("2009-01-01").getTime
        var endDate = format2.parse("2010-12-31").getTime
        var crDate = format.parse(line.getString(2)).getTime
        var maxGdp = 0
        var flag = false
        if (gdp != null) {
          if (crDate > startDate && crDate < endDate) {
            flag = true
          }
          flag
        }
        (crDate, gdp, line.getString(0))
      }
    }.map { rec =>
      {
        (rec._1, rec._2.max)
      }
    }.sortBy(rec => rec._2, false).take(10)
    result.foreach { println }
  }*/

  //5. Internet usage grown - Country where Internet usage has grown the most in the past decade
  var KPI5 = {
    val result = data.map { line =>
      {
        var internetUsage = line.getString(5)
        var usageNum = 0L
        if (internetUsage != null) {
          usageNum = Long.parseLong(internetUsage)
        }
        (usageNum, line.getString(0))
      }
    }.sortByKey(false).first()
    print(result)
  }

  spark.stop
}