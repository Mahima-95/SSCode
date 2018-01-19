package com.stack
//10. List of all the tags along with their counts
import org.apache.spark.sql.SparkSession
import scala.xml.XML

object KPI10 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Questions Count").master("local").getOrCreate()

    //Read some example file to a test RDD
    val data = spark.read.textFile("D:\\Mahima\\My Dev Space\\workspace\\WBI_Scala\\HP_And_SK\\Stack Overflow Data Analysis\\Posts.xml").rdd

    val result = data.filter { line => { line.trim().startsWith("<row") } }
      .filter { line => { line.contains("PostTypeId=\"1\"") } }
      .map { line =>
        {
          val xml = XML.loadString(line)
          (xml.attribute("Tags").get.toString())
        }
      }
      .flatMap { data =>
        {
          data.replaceAll("&lt;", " ").replaceAll("&gt;", " ").split(" ")
        }
      }
      .filter { tag => { tag.length() > 0 } }
      .map { data => { (data, 1) } }
      .reduceByKey(_ + _)
      .sortByKey(true)
      result.foreach { println }
      spark.stop
  }
}