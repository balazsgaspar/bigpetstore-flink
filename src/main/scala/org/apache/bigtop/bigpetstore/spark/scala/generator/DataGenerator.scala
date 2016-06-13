package org.apache.bigtop.bigpetstore.spark.scala.generator

import com.github.rnowling.bps.datagenerator._
import com.github.rnowling.bps.datagenerator.datamodels._
import com.github.rnowling.bps.datagenerator.framework.SeedFactory
import org.apache.bigtop.bigpetstore.flink.java.FlinkTransaction
import org.apache.bigtop.bigpetstore.flink.java.util.Utils
import org.apache.bigtop.bigpetstore.spark.scala._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable

object DataGenerator {
  def main(args: Array[String]) {

    // parse input parameters
    val parameters = Utils.parseArgs(args)
    val seed = parameters.getRequired("seed").toInt
    val numStores = parameters.getRequired("numStores").toInt
    val numCustomers = parameters.getRequired("numCustomers").toInt
    val burningTime = parameters.getRequired("burningTime").toDouble
    val simLength = parameters.getRequired("simLength").toDouble
    val output = parameters.getRequired("ETLInput")
    val master = parameters.getRequired("master")

    deleteDir(output)

    // Initialize context
    val startTime = java.lang.System.currentTimeMillis().toDouble / (24 * 3600 * 1000)

    val conf = new SparkConf(true).setMaster(master)
        .setAppName("DataGenerator")

    val sc = new SparkContext(conf)

    val inputData = new DataLoader().loadData()
    val seedFactory = new SeedFactory(seed)
    val productBroadcast = sc.broadcast(inputData.getProductCategories)

    val storeGenerator = new StoreGenerator(inputData, seedFactory)
    val stores = for (i <- 1 to numStores) yield storeGenerator.generate
    val storesBroadcast = sc.broadcast(stores)

    val customerGenerator = new CustomerGenerator(inputData, stores, seedFactory)
    val customers = for (i <- 1 to numCustomers) yield customerGenerator.generate

    // Generate transactions
    val transactions = sc.parallelize(customers)
      .mapPartitionsWithIndex{ (index : Int, customers: Iterator[Customer]) =>
        val seedFactory = new SeedFactory(index)
        val products = productBroadcast.value
        val stores = storesBroadcast.value
        val profile = new PurchasingProfileGenerator(products, seedFactory).generate

        customers.flatMap {
          case customer =>
            val transGen = new TransactionGenerator(customer, profile, stores, products, seedFactory)
            val transactions = new mutable.HashSet[FlinkTransaction]()
            var transaction = transGen.generate

            while (transaction.getDateTime < simLength) {
              if (transaction.getDateTime > burningTime) transactions.add(transaction)
              transaction = transGen.generate
            }
            transactions.iterator
        }

      }
      .map{t => t.setDateTime(t.getDateTime + startTime); t}

    transactions.saveAsTextFile(output)
  }
}
