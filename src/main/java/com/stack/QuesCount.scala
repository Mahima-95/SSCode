package com.stack

import java.text.SimpleDateFormat
import org.apache.spark.sql.SparkSession
import scala.xml.XML

object QuesCount {
  def main(args: Array[String]) {
    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    val format2 = new SimpleDateFormat("yyyy-MM");

    val spark = SparkSession.builder.appName("Monthly Questions Count").master("local").getOrCreate()

    //Read some example file to a test RDD
    val data = spark.read.textFile("D:\\Mahima\\My Dev Space\\workspace\\WBI_Scala\\HP_And_SK\\Stack Overflow Data Analysis\\Posts.xml").rdd

    var result = data.filter(line => { line.trim().startsWith("<row") })
      .filter(line => { line.contains("PostTypeId=\"1\"") })
      .flatMap(line => {
        var xml = XML.loadString(line)
        xml.attribute("CreationDate")
      })
      .map(line => {
        (format2.format(format.parse(line.toString())).toString(), 1)
      }).reduceByKey(_ + _)
    result.foreach { println }
    spark.stop
  }
}