package com.victormeyer.example.streaming.stockExample

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random


/**
 * Génère des cours boursiers fictifs minimalistes et les écrit dans un topic
 * Exemple de données {"id": 2, "price": 1.56, "symbol": "ABC"}
 */
object DataGenerator {

  val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  def createDataframe: DataFrame ={
    val ids: Seq[Int] = Seq.range(0, 1000, 1)
    val seqData: Seq[(String, String, Seq[String])] = ids.map(id => {
      (id.toString, generateRandomData(id), Seq("com.victormeyer.example.streaming.stockExample"))
    })
    seqData.toDF("key", "value", "headers")
  }

  def generateRandomData(id: Int): String ={
    val random: Random = Random
    val price: Double = random.nextDouble + random.nextInt(100).toDouble
    val symbol: String = random.nextString(3)
    s"{\"id\": $id, \"price\": $price, \"symbol\": \"$symbol\"}"
  }

}
