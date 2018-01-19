package com.stack
//11. Number of question with specific tags (nosql, big data) which was asked in the specified time range
//(from 01-01-2015 to 31-12-2015)

import org.apache.spark.sql.SparkSession
import scala.xml.XML
import java.text.SimpleDateFormat

object KPI11 {
  def main(args: Array[String]) = {

    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    val format2 = new SimpleDateFormat("yyyy-MM")
    val format3 = new SimpleDateFormat("yyyy-MM-dd")

    var startTime = format3.parse("2015-01-01").getTime
    var endTime = format3.parse("2015-12-31").getTime
    val spark = SparkSession.builder.appName("Questions Count").master("local").getOrCreate()

    //Read some example file to a test RDD
    val data = spark.read.textFile("D:\\Mahima\\My Dev Space\\workspace\\WBI_Scala\\HP_And_SK\\Stack Overflow Data Analysis\\Posts.xml").rdd

    val result = data.filter { line => { line.trim().startsWith("<row") } }
      .filter { line => { line.contains("PostTypeId=\"1\"") } }
      .map { line =>
        {
          val xml = XML.loadString(line)
          val crDate = xml.attribute("CreationDate").get.toString()
          val tags = xml.attribute("Tags").get.toString()
          (crDate, tags, line)
        }
      }
      .filter { data =>
        {
          var flag = false
          val crTime = format.parse(data._1.toString()).getTime
          if (crTime > startTime && crTime < endTime && (data._2.toLowerCase().contains("bigdata") ||
            data._2.toLowerCase().contains("spark") || data._2.toLowerCase().contains("hadoop")))
            flag = true
          flag
        }
      }
    result.foreach(println)
    print(result.count())
    spark.stop
  }
}