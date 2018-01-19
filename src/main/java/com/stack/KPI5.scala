package com.stack
//5. The questions that doesn't have any answers  Number of questions with  0 number of answers
import org.apache.spark.sql.SparkSession
import scala.xml.XML

object KPI5 {
  def main(args: Array[String]) = {
    System.setProperty("hadoop.home.dir", "D:\\Mahima\\transferData\\spark\\cdh5")
    //  System.setProperty("spark.sql.warehouse.dir", "file:/D:/Mahima/transferData/spark/spark-2.0.2-bin-hadoop2.7/spark-2.0.2-bin-hadoop2.7/spark-warehouse")
    val spark = SparkSession.builder.appName("Questions Count").master("local").getOrCreate()

    //Read some example file to a test RDD
    val data = spark.read.textFile("D:\\Mahima\\My Dev Space\\workspace\\WBI_Scala\\HP_And_SK\\Stack Overflow Data Analysis\\Posts.xml").rdd
    val result = data.filter { line => { line.trim().startsWith("<row") }
    }
      .filter { line => { line.contains("PostTypeId=\"1\"") }
      }.map { line =>
        {
          val xml = XML.loadString(line)
          (Integer.parseInt(xml.attribute("AnswerCount").getOrElse(0).toString()), line)
        }
      }.filter {
        x => { x._1 == 0 }
      }
    result.foreach { println }
    println(result.count())

    spark.stop

  }
}