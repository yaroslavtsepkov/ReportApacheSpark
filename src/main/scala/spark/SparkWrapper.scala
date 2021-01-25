package spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

trait SparkWrapper {
  val cfg = new SparkConf()
    .setAppName("SparkApp")
    .setMaster("local[*]")

  val sc = new SparkContext(cfg)

  val spark = SparkSession
    .builder()
    .getOrCreate()

}
