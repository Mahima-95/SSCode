package com.settopbox.analysis

import org.apache.spark.sql.SparkSession
import scala.xml.XML

object KPI1_1_2nd_way {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("set top box").master("local").getOrCreate();
    val data = spark.read.textFile("D:\\Mahima\\My Dev Space\\workspace\\WBI_Scala\\HP_And_SK\\Setup box Analysis\\Set_Top_Box_Data.txt").rdd
    val result = data.map(line => {
      var tokens = line.split("\\^")
      (Integer.parseInt(tokens(2)), line)
    })
      .filter(data => {
        data._1.==(100)
      })
      .map(data => {
        var xmlTags = data._2.split("\\^")
        (xmlTags(4), data)
      })
      .map(data => {
        var xml = XML.loadString(data._1)
        var innerTags = xml \\ "nv"
        var xml0 = XML.loadString(innerTags.theSeq(3).toString())
        var duration = xml0.attribute("v").get.toString()
        (duration, data._2)
      }).groupByKey()
      .map(data => {
        (data._1.max, data._2)
      }).sortBy(rec => rec._2, false).take(5)
    result.foreach(println)
    spark.stop
  }
}