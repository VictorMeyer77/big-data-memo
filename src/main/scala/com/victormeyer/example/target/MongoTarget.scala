package com.victormeyer.example.target

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import scala.collection.mutable

class MongoTarget(uri: String, database: String, collection: String)(implicit spark: SparkSession) {

  def getExistsMongoRecord(df: DataFrame, keys: Seq[String]): DataFrame ={
    spark.read
      .format("mongodb")
      .option("connection.uri", uri)
      .option("database", database)
      .option("collection", collection)
      .load()
      .filter(makeFilterKeysColumn(df, keys))
  }

  /**
   * Créer un filtre à appliquer au reader mongo pour ne récupérer que les records dont les clefs sont présentes dans le dataframe
   * @param df Dataframe d'entrée
   * @param keys liste des clefs
   * @return filtre spark
   */
  private def makeFilterKeysColumn(df: DataFrame, keys: Seq[String]): Column ={
    val keysDfValues: Seq[Row] = df.select(keys.head, keys.tail: _*).collect()
    val filters: mutable.ArrayBuffer[Column] = mutable.ArrayBuffer()
    keysDfValues.foreach(kv => {
      var rowFilter: Column = col(keys.head) === kv.get(0)
      for(i <- 1 until keys.size){
        rowFilter = rowFilter && col(keys(i)) === kv.get(i)
      }
      filters += rowFilter
    })
    filters.reduce{(acc, f) => acc || f}
  }

}
