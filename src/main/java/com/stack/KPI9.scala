package com.stack

import org.apache.spark.sql.SparkSession
import scala.xml.XML

//9. The most scored questions with specific tags � Questions having tag hadoop, spark in descending order of score
object KPI9 {
  def main(args: Array[String]) = {

    val spark = SparkSession.builder.appName("Questions Count").master("local").getOrCreate()

    //Read some example file to a test RDD
    val data = spark.read.textFile("D:\\Mahima\\My Dev Space\\workspace\\WBI_Scala\\HP_And_SK\\Stack Overflow Data Analysis\\Posts.xml").rdd

    val result = data.filter { line => { line.trim().startsWith("<row") } }
      .filter { line => { line.contains("PostTypeId=\"1\"") } }
      .map { line =>
        {
          val xml = XML.loadString(line)
          (xml.attribute("Tags").get.toString(), Integer.parseInt(xml.attribute("Score").get.toString()), line)
        }
      }
      .filter { tag => { tag._1.contains("spark") } }
      .map { data => { (data._2, data._3) }
      }
      .sortByKey(false)
    result.foreach { println }
    spark.stop
  }
}