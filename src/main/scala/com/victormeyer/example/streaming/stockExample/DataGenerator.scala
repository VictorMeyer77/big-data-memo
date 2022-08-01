package com.victormeyer.example.streaming.stockExample
import org.apache.spark.sql.types.{ArrayType, BinaryType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.util.Random


/**
 * Génère des cours boursiers fictifs minimalistes et les écrit dans un topic
 * Exemple de données {"id": 2, "price": 1.56, "symbol": "ABC"}
 */
object DataGenerator {

  val spark: SparkSession = SparkSession
    .builder
    .appName("Stock Data Generator")
    .getOrCreate()

  def createDataframe: DataFrame ={
    val schema: StructType = StructType(Seq(
        StructField("key", StringType, nullable=false),
        StructField("value", StringType, nullable=false),
        StructField("headers", ArrayType(StructType(Seq(StructField("key", StringType, nullable=false), StructField("value", BinaryType, nullable=false)))))
      )
    )
    val ids: Seq[Int] = Seq.range(0, 10, 1)
    val seqData: Seq[Row] = ids.map(id => {
      Row(id.toString, generateRandomData(id), Seq(Row("class", "com.victormeyer.example.streaming.stockExample".getBytes())))
    })
    spark.createDataFrame(spark.sparkContext.parallelize(seqData), schema)
  }

  def generateRandomData(id: Int): String ={
    val random: Random = Random
    val price: String = (random.nextDouble + random.nextInt(100).toDouble).toString.replace(",", ".")
    val symbol: String = Random.alphanumeric.take(3).mkString.toUpperCase
    "{\"id\": %d, \"price\": %s, \"symbol\": \"%s\"}".format(id, price, symbol)
  }

  def writeToKafka(): Unit ={
    createDataframe
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "stock_topic_1")
      .save()
  }

}
