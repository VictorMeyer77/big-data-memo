package com.victormeyer.example.streaming.stockExample

import com.victormeyer.example.target.MongoTarget
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{col, desc, explode, from_json, rank, window}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object MongoInsert {

  val spark: SparkSession = SparkSession
    .builder
    .appName("Stock Mongo Insert")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val stockValueSchema: StructType = StructType(Seq(
    StructField("id", IntegerType, nullable=false),
    StructField("price", DoubleType, nullable=false),
    StructField("symbol", StringType, nullable=false)
  ))

  def insert(): Unit ={

    val df: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "stock_topic_1")
      .option("includeHeaders", "true")
      .option("startingOffsets", """{"stock_topic_1":{"0":4000}}""")
      .load()

    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "headers", "timestamp")
      .as[(String, String, Array[(String, Array[Byte])], String)]
      .toDF()
      //.filter("key == 5")
      .writeStream
      //.format("console")
      .option("checkpointLocation", "file:////home/victor/Entreprise/Developpement/BigData/big-data-memo/checkpoints/stock/mongo")
      .foreachBatch((batchDF: DataFrame, batchId: Long) => {
         println(s"batch id: $batchId")
         val df: DataFrame = deserializeDf(batchDF, stockValueSchema)
         writeToMongo(df.select("id", "price", "symbol"), Seq())
      }
      )
      .start()
      .awaitTermination()

  }

  def writeToMongo(df: DataFrame, keys: Seq[String]): Unit ={

    /*df.write
      .format("mongodb")
      .mode("append")
      .option("connection.uri", "mongodb://mongo:mongo@localhost:27017/")
      .option("database", "test")
      .option("collection", "stock_photo")
      .save()*/
    val mongoTarget: MongoTarget = new MongoTarget("mongodb://mongo:mongo@localhost:27017/", "test", "stock_photo")(spark)
    mongoTarget.getExistsMongoRecord(df, keys).show()

  }

  def deserializeDf(df: DataFrame, valueStruct: StructType): DataFrame ={

    df.withColumn("value", from_json(col("value"), valueStruct))
      //.withColumn("headers", explode(col("headers")))
      .select(col("key"),
              col("timestamp"),
      //        col("headers.*"),
              col("value.*"))
  }

}