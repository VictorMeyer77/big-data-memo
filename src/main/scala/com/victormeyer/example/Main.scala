package com.victormeyer.example

import com.victormeyer.example.streaming.stockExample.{DataGenerator, MongoInsert}

object Main {

  def main(args: Array[String]): Unit ={

    args(0).toLowerCase match {
      case "stock" =>
        args(1).toLowerCase match {
          case "generator" => DataGenerator.writeToKafka()
          case "mongo" => MongoInsert.insert()
        }
    }
  }
}
