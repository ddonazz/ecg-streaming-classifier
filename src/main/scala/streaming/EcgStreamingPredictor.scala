package it.utiu.thesis
package streaming

import utils.Config

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.{DataFrame, SparkSession}

object EcgStreamingPredictor {
  def main(args: Array[String]): Unit = {
    println("Inizializzazione Spark Streaming per Inferenza ECG...")

    val spark = SparkSession.builder()
      .appName("EcgStreamingPredictor")
      .master(Config.SPARK_MASTER)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val modelPath = Config.ML_MODEL_PATH
    println(s"Caricamento del modello di Machine Learning da: $modelPath ...")
    val pipelineModel = PipelineModel.load(modelPath)

    val datasetPath = Config.DATASET_PATH
    val jsonSchema = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(datasetPath)
      .schema

    println(s"Connessione in corso ad Apache Kafka sul topic '${Config.KAFKA_TOPIC}'...")
    val kafkaStreamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", Config.KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", Config.KAFKA_TOPIC)
      .option("startingOffsets", "earliest")
      .load()

    val parsedStreamDF = kafkaStreamDF
      .selectExpr("CAST(value AS STRING) as json_string")
      .select(from_json(col("json_string"), jsonSchema).alias("data"))
      .select("data.*")

    val predictionsStreamDF = pipelineModel.transform(parsedStreamDF)

    val finalStreamDF = predictionsStreamDF.select(
      col("record"),
      col("type").alias("true_label"),
      col("prediction"),
      col("probability")
    )

    val deltaPath = Config.DELTA_PREDICTIONS_PATH
    val checkpointPath = Config.STREAMING_CHECKPOINT_PATH

    val query = finalStreamDF.writeStream
      .outputMode("append")
      .option("checkpointLocation", checkpointPath)
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.persist()

        if (!batchDF.isEmpty) {
          println(s"\n--- Elaborazione e Salvataggio Batch: $batchId ---")

          batchDF.show(20, truncate = false)

          batchDF.write
            .format("delta")
            .mode("append")
            .save(deltaPath)
        }

        batchDF.unpersist()
        ()
      }
      .start()

    println("\n=== Sistema di Streaming Avviato e in Ascolto (Multi-Sink Sicuro)! ===")
    println("Avvia il Simulatore (EcgPatientSimulator) per inviare i dati...\n")

    spark.streams.awaitAnyTermination()
  }
}