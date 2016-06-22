package org.apache.bigtop.bigpetstore.spark.scala.recommender

import org.apache.bigtop.bigpetstore.flink.java.util.Utils
import org.apache.bigtop.bigpetstore.spark.scala._
import org.apache.spark.mllib.recommendation._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamingRecommender {
  def main(args: Array[String]) {

    val parameters = Utils.parseArgs(args)
    val customerOut = parameters.getRequired("customerOut")

    val master = parameters.getRequired("master")

    val conf = new SparkConf(true).setMaster(master)
      .setAppName("Spark ALS")
      .set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(conf, Seconds(1))

    // Load the model
    val model = MatrixFactorizationModel.load(sc, customerOut)

    println(model.predict(0,0))

    val query = ssc.socketTextStream("localhost", 9999)
      .map(user => user.toInt)

//    val result = query.foreachRDD{ rdd =>
//      val userProducts = rdd.map(user => (user, 0))
//      val rec = model.predict(userProducts)
////      rec.collect.foreach(println)
////      rec.saveAsTextFile(""
//    }

    val result2 = query.transform{ rdd =>
      val userProducts = rdd.map(user => (user, 0))
      model.predict(userProducts)
    }

    result2.saveAsTextFiles("/tmp/rec-out")

    ssc.start()
    ssc.awaitTermination()

  }
}
