package org.apache.bigtop.bigpetstore.spark.scala.recommender

import org.apache.bigtop.bigpetstore.flink.java.util.Utils
import org.apache.bigtop.bigpetstore.spark.scala._
import org.apache.spark.mllib.recommendation._
import org.apache.spark.{SparkConf, SparkContext}

object FactorModel {
  def main(args: Array[String]) {

    val parameters = Utils.parseArgs(args)
    val inputFile = parameters.getRequired("ETLOutput")
    val iterations = parameters.getRequired("iterations").toInt
    val numFactors = parameters.getRequired("numFactors").toInt
    val lambda = parameters.getRequired("lambda").toDouble
    val alpha = 20.0
    val customerOut = parameters.getRequired("customerOut")
    val productOut = parameters.getRequired("productOut")

    val master = parameters.getRequired("master")

    deleteDir(customerOut)
    deleteDir(productOut)

    val conf = new SparkConf(true).setMaster(master)
      .setAppName("Spark ALS")

    val sc = new SparkContext(conf)

    // Read and parse the input data
    val input = sc.textFile(inputFile)
      .map(s => Rating(s.split(",")(0).toInt, s.split(",")(1).toInt, 1.0))

    // Create a model using MLlib
    val model = ALS.trainImplicit(input, numFactors, iterations, lambda, alpha)

    model.save(sc, customerOut)
  }
}
