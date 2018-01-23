package com.settopbox.analysis

import org.apache.spark.sql.SparkSession
import scala.xml.XML

object KPI2_1 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("set top box").master("local").getOrCreate();
    val data = spark.read.textFile("D:\\Mahima\\My Dev Space\\workspace\\WBI_Scala\\HP_And_SK\\Setup box Analysis\\Set_Top_Box_Data.txt").rdd
    val result = data.map(line => {
      var tokens = line.split("\\^")
      (tokens, line)
    }).filter(data => {
      data._1(2).equals("101")
    }).filter(data => {
      data._1(4).contains("n=\"PowerState\" v=\"ON\"").||(data._1(4).contains("n=\"PowerState\" v=\"OFF\""))
    }).map(data => {
      var xml = XML.loadString(data._1(4))
      var innerTags = xml \\ "nv"
      var powerState: String = null
      if (!innerTags.isEmpty) {
        powerState = innerTags.theSeq(1).attribute("v").get.toString()
      }
      (powerState, data._2)
    }).sortByKey(false)
    result.foreach(println)
    println(result.count())
    spark.stop
  }
}