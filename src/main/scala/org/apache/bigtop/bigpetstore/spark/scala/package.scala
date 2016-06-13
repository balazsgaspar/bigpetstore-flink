package org.apache.bigtop.bigpetstore.spark

import com.github.rnowling.bps.datagenerator.datamodels.Transaction
import org.apache.bigtop.bigpetstore.flink.java.FlinkTransaction

package object scala {
  implicit def transactionToFlink(transaction: Transaction) : FlinkTransaction = new FlinkTransaction(transaction)

  def deleteDir(dirPath : String) : Unit = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(dirPath), hadoopConf)
    try { hdfs.delete(new org.apache.hadoop.fs.Path(dirPath), true) } catch { case _ : Throwable => { } }

  }
}
