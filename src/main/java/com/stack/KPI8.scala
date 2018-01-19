package com.stack
//8. The distribution of number of questions closed per month
import java.text.SimpleDateFormat
import org.apache.spark.sql.SparkSession
import scala.xml.XML

object KPI8 {
  def main(args: Array[String]) = {
    //    System.setProperty("hadoop.home.dir", "D:\\Anish_Training_Data\\setups\\hadoop_setups\\CDH5\\hadoop-2.5.0-cdh5.3.2")
    //    System.setProperty("spark.sql.warehouse.dir", "file:/D:/Anish_Training_Data/setups/Spark_Setups/spark-2.0.0-bin-hadoop2.6/spark-warehouse")

    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    val format2 = new SimpleDateFormat("yyyy-MM");

    val spark = SparkSession
      .builder
      .appName("Closed Questions")
      .master("local")
      .getOrCreate()

    //Read some example file to a test RDD
    val data = spark.read.textFile("D:\\Mahima\\My Dev Space\\workspace\\WBI_Scala\\HP_And_SK\\Stack Overflow Data Analysis\\Posts.xml").rdd

    val result = data.filter { line => { line.trim().startsWith("<row") }
    }
      .filter { line => { line.contains("PostTypeId=\"1\"") }
      }
      .map { line =>
        {
          var xml = XML.loadString(line)

          var closeDate = " "
          if (xml.attribute("ClosedDate") != None) {
            var clDate = xml.attribute("ClosedDate").get.toString()
            closeDate = format2.format(format.parse(clDate))
          }
          (closeDate, 1)
        }
      }
      .filter { data => { data._1.length() > 0 } }
      .reduceByKey(_ + _)

    result.foreach { println }

    spark.stop
  }
}