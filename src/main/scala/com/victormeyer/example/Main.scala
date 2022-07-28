package com.victormeyer.example

import com.victormeyer.example.streaming.stockExample.DataGenerator

object Main {

  def main(args: Array[String]): Unit ={

    DataGenerator.createDataframe.show()

  }

}
