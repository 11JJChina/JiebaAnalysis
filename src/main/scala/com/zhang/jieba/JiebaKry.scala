package com.zhang.jieba

import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

object JiebaKry {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().registerKryoClasses(Array(classOf[JiebaSegmenter]))
      .set("spark.rpc.message.maxSize", "800")

    val spark = SparkSession.builder()
      .appName("JiebaKry")
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()

    def jieba_seg(df: DataFrame, colname: String): DataFrame = {
      val segmenter = new JiebaSegmenter()
      val seg = spark.sparkContext.broadcast(segmenter)
      val jieba_udf = udf { (sentence: String) =>
        val segV = seg.value
        segV.process(sentence.toString, SegMode.INDEX)
          .toArray()
          .map(_.asInstanceOf[SegToken].word)
          .filter(_.length > 1)
      }
      //seg列出来的数据都是Array[String]
      df.withColumn("seg", jieba_udf(col(colname)))
    }

    val df = spark.sql("select content,label from default.new_no_seg limit 300")
    val df_seg = jieba_seg(df,"content")
    df_seg.show()
    df_seg.write.mode("overwrite").saveAsTable("default.news_jieba")

  }

}
