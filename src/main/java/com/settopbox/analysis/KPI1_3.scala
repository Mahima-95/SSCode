package com.settopbox.analysis
//iii. Total number of devices with ChannelType="LiveTVMediaChannel"
import org.apache.spark.sql.SparkSession
import scala.xml.XML

object KPI1_3{

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("set top box").master("local").getOrCreate();
    val data = spark.read.textFile("D:\\Mahima\\My Dev Space\\workspace\\WBI_Scala\\HP_And_SK\\Setup box Analysis\\Set_Top_Box_Data.txt").rdd
    val result = data.map(line => {
      var tokens = line.split("\\^")
      (tokens, line)
    }).filter(data => {
      data._1(2).equals("100")
    }).filter(line => {
      line._1(4).contains("n=\"ChannelType\" v=\"LiveTVMediaChannel\"")
    }).map(data => {
      var xml = XML.loadString(data._1(4))
      var innerTags = xml \\ "nv"
      var ChannelType: String = null
      if (!innerTags.isEmpty) {
        ChannelType = innerTags.theSeq(6).attribute("v").get.toString()
      }
      (ChannelType, data._2)
    }).sortByKey(false)
    result.foreach(println)
    println(result.count())
    spark.stop
  }
}
