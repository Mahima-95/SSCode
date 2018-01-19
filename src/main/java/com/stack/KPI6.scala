package com.stack

import org.apache.spark.sql.SparkSession
import scala.xml.XML

//6. Number of questions with more than 2 answers
object KPI6 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Questions Count").master("local").getOrCreate()

    //Read some example file to a test RDD
    val data = spark.read.textFile("D:\\Mahima\\My Dev Space\\workspace\\WBI_Scala\\HP_And_SK\\Stack Overflow Data Analysis\\Posts.xml").rdd

    val result = data.filter { line => { line.trim().startsWith("<row") } }
      .filter { line => line.contains("PostTypeId=\"1\"") }
      .map { line =>
        {
          var xml = XML.loadString(line)
          (Integer.parseInt(xml.attribute("AnswerCount").getOrElse(0).toString()), line)
        }
      }
      .filter { x => { x._1 > 2 } }
      .sortByKey(false)
    result.foreach { println }

    spark.stop
  }
}