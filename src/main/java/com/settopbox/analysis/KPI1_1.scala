package com.settopbox.analysis

import org.apache.spark.sql.SparkSession
import scala.xml.XML

object KPI1_1 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("set top box").master("local").getOrCreate();
    val data = spark.read.textFile("D:\\Mahima\\My Dev Space\\workspace\\WBI_Scala\\HP_And_SK\\Setup box Analysis\\Set_Top_Box_Data.txt").rdd
    val result = data.map(line => {
      var tokens = line.split("\\^")
      (tokens, line)
    }).filter(data => {
      data._1(2).equals("100")
    }).filter(data => {
      data._1(4).contains("n=\"Duration\"")
    }).map(line => {
      var xml = XML.loadString(line._1(4))
      var innerTags = xml \\ "nv"
      var duration: Int = 0
      if (innerTags.theSeq(2).toString().contains("n=\"Duration\"")) {
        duration = innerTags.theSeq(2).attribute("v").get.toString().toInt
      } else if (innerTags.theSeq(3).toString().contains("n=\"Duration\"")) {
        duration = innerTags.theSeq(3).attribute("v").get.toString().toInt
      }
      val tokens = line._2.split("\\^")
      (duration, tokens(5))
    }).sortByKey(false).take(5)
    result.foreach(println)
    spark.stop
  }
}