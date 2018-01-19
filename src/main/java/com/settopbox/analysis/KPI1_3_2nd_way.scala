package com.settopbox.analysis
//iii. Total number of devices with ChannelType="LiveTVMediaChannel"
import org.apache.spark.sql.SparkSession

object KPI1_3_2nd_way {
 
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
        val xmlTokens = data._2.split("\\^")
        (xmlTokens(4), data)
      })
      .filter(line => {
        line._1.contains("\"ChannelType\" v=\"LiveTVMediaChannel\"")
      })
      .sortByKey(false).count()
    println(result)
    spark.stop
  }
}
//i think, this one is correct
 //ans=662