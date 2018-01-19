package com.stack
//2.Monthly questions count –provide the distribution of number of questions asked per month
import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import scala.xml.XML

object KPI2 {
  def main(args: Array[String]) {
    //System.setProperty("hadoop.home.dir", "D:\\Mahima\\transferData\\spark\\cdh5")
    // System.setProperty("spark.sql.warehouse.dir", "file:/D:/Mahima/transferData/spark/spark-2.0.2-bin-hadoop2.7/spark-2.0.2-bin-hadoop2.7/spark-warehouse")

    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    val format2 = new SimpleDateFormat("yyyy-MM");

    val spark = SparkSession.builder.appName("Monthly Questions Count").master("local").getOrCreate()

    //Read some example file to a test RDD
    val data = spark.read.textFile("D:\\Mahima\\My Dev Space\\workspace\\WBI_Scala\\HP_And_SK\\Stack Overflow Data Analysis\\Posts.xml").rdd

    val result = data.filter { line => { line.trim().startsWith("<row") } }
      .filter { line => { line.contains("PostTypeId=\"1\"") } }
      .flatMap { line =>
        {
          val xml = XML.loadString(line)
          xml.attribute("CreationDate")
        }
      }
      .map { line =>
        {
          (format2.format(format.parse(line.toString())).toString(), 1)
        }
      }
      .reduceByKey(_ + _)
    result.foreach { println }
    spark.stop
  }
}