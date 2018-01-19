package com.stack
//7. Number of questions which are active for more than 6 months
import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import scala.xml.XML

object KPI7 {
  def main(args: Array[String]) {
    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    val spark = SparkSession.builder.appName("QuesActive").master("local").getOrCreate()

    //Read some example file to a test RDD
    val data = spark.read.textFile("D:\\Mahima\\My Dev Space\\workspace\\WBI_Scala\\HP_And_SK\\Stack Overflow Data Analysis\\Posts.xml").rdd

    val result = data.filter { line => { line.trim().startsWith("<row") } }
      .filter { line => line.contains("PostTypeId=\"1\"") }
      .map { line =>
        {
          var xml = XML.loadString(line)
          (xml.attribute("CreationDate").get, xml.attribute("LastActivityDate").get, line)
        }
      }
      .map { data =>
        {
          var crDate = format.parse(data._1.text)
          var crTime = crDate.getTime

          var endDate = format.parse(data._2.text)
          var endTime = endDate.getTime
          var timeDiff = endTime - crTime
          (crDate, endDate, timeDiff, data._3)
        }
      }
      .filter { data => { data._3 / (1000 * 60 * 60 * 24) > 30 * 6 } }

    result.foreach { println }
    println(result.count())
    //			println(result.count())

    spark.stop

  }
}