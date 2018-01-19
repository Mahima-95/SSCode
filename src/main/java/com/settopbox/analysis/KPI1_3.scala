//iii. Total number of devices with ChannelType="LiveTVMediaChannel"
package com.settopbox.analysis
import org.apache.spark.sql.SparkSession
import scala.xml.XML

object KPI1_3 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("set top box").master("local").getOrCreate();
    val data = spark.read.textFile("D:\\Mahima\\My Dev Space\\workspace\\WBI_Scala\\HP_And_SK\\Setup box Analysis\\Set_Top_Box_Data.txt").rdd
    val result = data.map(line => {
      var tokens = line.split("\\^")
      (Integer.parseInt(tokens(2)), line)
    }).filter(line => {
      line._1.==(100)
    }).map(data => {
      val newTokens = data._2.split("\\^")
      (newTokens(4), data)
    }).map(data => {
      var xml = XML.loadString(data._1)
      var innerTags = xml \\ "nv"
      var xml0 = XML.loadString(innerTags.theSeq(6).toString())
      var duration = xml0.attribute("v").get.toString()
      if (duration == "LiveTVMediaChannel") {
        duration
      }
      (data, duration)
    }).sortByKey(false).count()
    print(result)
    spark.stop
  }
}

//i think, it gives me wrong answer
//ans=693