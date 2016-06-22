package org.apache.bigtop.bigpetstore.spark.scala.etl

import java.util.Date

import org.apache.bigtop.bigpetstore.flink.java.FlinkTransaction
import org.apache.bigtop.bigpetstore.flink.java.util.Utils
import org.apache.bigtop.bigpetstore.spark.scala._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

object ETL {
  def main(args: Array[String]) {

    // parse input parameters
    val parameters = Utils.parseArgs(args)
    val numStores = parameters.getRequired("numStores").toInt
    val numCustomers = parameters.getRequired("numCustomers").toInt
    val simLength = parameters.getRequired("simLength").toDouble
    val input = parameters.getRequired("ETLInput")
    val output = parameters.getRequired("ETLOutput")
    val master = parameters.getRequired("master")

    deleteDir(output)

    // Initialize context
    val conf = new SparkConf(true).setMaster(master)
      .setAppName("DataGenerator")

    val sc = new SparkContext(conf)

    val transactions = sc.textFile(input)
      .map(new FlinkTransaction(_))

    // Generate unique product IDs
    val productsWithIndex = transactions.flatMap(t => t.getProducts)
      .distinct
      .zipWithUniqueId

    // Generate the customer-product pairs
    val customerAndProductIDS = transactions.flatMap(t => t.getProducts.map(p => (t.getCustomer.getId, p)))
      .map(pair => (pair._2, pair._1.toLong))
      .join(productsWithIndex)
      .map(res => (res._2._1, res._2._2))
      .distinct

    customerAndProductIDS
      .map(pair => Array(pair._1, pair._2).mkString(","))
      .saveAsTextFile(output)

    //Print stats on the generated dataset
    val numProducts = productsWithIndex.count
    val filledFields = customerAndProductIDS.count
    val sparseness = 1 - (filledFields.toDouble / (numProducts * numCustomers))

    // Statistics with SparkSQL
    val sqlContext = new SQLContext(sc)

    val dataFrame = sqlContext.read.json(input)

    dataFrame.registerTempTable("transactions")

    // Transaction count of stores
    val storeTransactionsCount = sqlContext.sql("SELECT store.id id, store.name name, COUNT(store.id) count " +
      "FROM transactions " +
      "GROUP BY store.id, store.name")

    storeTransactionsCount.registerTempTable("storeTransactions")

    // Store(s) with the most transactions
    val bestStores = sqlContext.sql("SELECT st.id, st.name, max.count " +
      "FROM storeTransactions st, (SELECT MAX(count) as count FROM storeTransactions) max " +
      "WHERE st.count = max.count")
      .collect

    // Transaction count of months
    def month = (dateTime : Double) => {
      val millis = (dateTime * 24 * 3600 * 1000).toLong
      new Date(millis).getMonth
    }

    sqlContext.udf.register("month", month)

    val monthTransactionCount = sqlContext.sql("SELECT month(dateTime) month, COUNT(month(dateTime)) count " +
      "FROM transactions " +
      "GROUP BY month(dateTime)")
      .collect

    println("Generated bigpetstore stats")
    println("---------------------------")
    println("Customers:\t" + numCustomers)
    println("Stores:\t\t" + numStores)
    println("simLength:\t" + simLength)
    println("Products:\t" + numProducts)
    println("sparse:\t\t" + sparseness)
    println
    println("Store(s) with the most transactions")
    println("---------------------------")
    bestStores.foreach(println)
    println
    println("Monthly transaction count")
    println("---------------------------")
    monthTransactionCount.foreach(println)
  }
}
