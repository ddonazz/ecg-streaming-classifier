package it.utiu.thesis
package batch

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}

object EcgModelTrainer {
  def main(args: Array[String]): Unit = {
    println("Inizializzazione Spark per classificazione ECG...")

    val spark = SparkSession.builder()
      .appName("EcgModelTrainer")
      .master("local[*]")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.codegen.wholeStage", "false")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println(s"Spark avviato con successo! Versione: ${spark.version}")

    val datasetPath = "/home/andrea/data/ecg/*.csv"

    println(s"Leggendo i dataset da: $datasetPath")
    val rawDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(datasetPath)

    println(s"Numero totale di battiti caricati: ${rawDF.count()}")

    println("Tipi di patologie nel dataset:")
    rawDF.groupBy("type").count().show()

    println("Inizio Data Preparation...")

    val indexer = new StringIndexer()
      .setInputCol("type")
      .setOutputCol("label")
      .fit(rawDF)

    val indexedDF = indexer.transform(rawDF)

    val featureCols = indexedDF.columns.filter(colName =>
      colName != "record" && colName != "type" && colName != "label"
    )

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    val assembledDF = assembler.transform(indexedDF)

    val finalDF = assembledDF.select("features", "label")

    println("Dati pronti per il Machine Learning (Prime 5 righe):")
    finalDF.show(5, truncate = false)

    spark.stop()
  }
}