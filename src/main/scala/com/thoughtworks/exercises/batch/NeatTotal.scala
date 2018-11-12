package com.thoughtworks.exercises.batch

import java.util.Properties

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object NeatTotal {
  def main(args: Array[String]): Unit = {
    val log = LogManager.getLogger(this.getClass)

    val properties = new Properties()
    properties.load(this.getClass.getResourceAsStream(s"/application.properties"))
    val baseBucket = properties.getProperty("base_bucket")
    val username = properties.get("username")
    val dataFilesBucket = properties.getProperty("data_files_bucket")

    val ordersBucket = s"$baseBucket/$username/$dataFilesBucket/orders"
    val orderItemsBucket = s"$baseBucket/$username/$dataFilesBucket/orderItems"
    val productsBucket = s"$baseBucket/$username/$dataFilesBucket/products"

    val spark = SparkSession
      .builder()
//      .master("local")
      .appName("Data Engineering Capability Development - ETL Exercises")
      .getOrCreate()

    val orderItemsDF = spark.read
        .option("delimiter", ";")
        .option("header", true)
        .option("infer_schema", true)
        .csv(orderItemsBucket)

    orderItemsDF.show(false)

    val productsDF = spark.read
      .option("delimiter", ";")
      .option("header", true)
      .option("infer_schema", true)
      .csv(productsBucket)

    productsDF.show(false)

    val joinOrderItensWithProducts = orderItemsDF.join(productsDF, Seq("ProductId"))

      joinOrderItensWithProducts.show(false)

    joinOrderItensWithProducts.selectExpr("sum((Price-Discount)*Quantity) as Total").show(false)



    //185.670.050.745
    //cento e oitenta e cinco bilhões, seiscentos e setenta milhões, cinquenta mil e setecentos e quarenta e cinco
  }
}
