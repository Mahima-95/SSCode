package com.stack
//12. Average time for a post to get a correct answer
import java.text.SimpleDateFormat
import org.apache.spark.sql.SparkSession
import scala.xml.XML

object KPI12 {
  def main(args: Array[String]) {

    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    val format2 = new SimpleDateFormat("yyyy-MM");

    val spark = SparkSession
      .builder
      .appName("AvgAnsTime")
      .master("local")
      .getOrCreate()

    //Read some example file to a test RDD
    val data = spark.read.textFile("D:\\Mahima\\My Dev Space\\workspace\\WBI_Scala\\HP_And_SK\\Stack Overflow Data Analysis\\Posts.xml").rdd

    val baseData = data.filter { line => { line.trim().startsWith("<row") } }
      .map { line =>
        {
          var xml = XML.loadString(line)
          var aaId = ""
          if (xml.attribute("AcceptedAnswerId") != None) {
            aaId = xml.attribute("AcceptedAnswerId").get.toString()
          }
          var crDate = xml.attribute("CreationDate").get.toString()
          var rId = xml.attribute("Id").get.toString()
          (rId, aaId, crDate)
        }
      }
    var aaData = baseData.map { data => { (data._2, data._3) } }
      .filter { data => { (data._1.length() > 0) } }
    var rData = baseData.map { data => { (data._1, data._3) } }
    var joinData = rData.join(aaData)
      .map { data =>
        {
          var queDate = format.parse(data._2._2).getTime
          var ansDate = format.parse(data._2._1).getTime
          var diff: Float = ansDate - queDate
          var time: Float = diff / (1000 * 60 * 60)
          time
        }
      }
    var count = joinData.count()
    var result = joinData.sum() / count
    println(result)
    spark.stop
  }
}