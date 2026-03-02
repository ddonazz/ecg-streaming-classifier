package it.utiu.thesis
package utils

object Config {
  // ==========================================
  // CONFIGURAZIONI KAFKA E RETE
  // ==========================================
  val KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
  val KAFKA_TOPIC = "ecg-input-stream"
  val SPARK_MASTER = "local[*]"

  // ==========================================
  // PERCORSI FILE SYSTEM (Dati e Modelli)
  // ==========================================
  val DATASET_PATH = "/home/andrea/data/ecg/*.csv"
  val ML_MODEL_PATH = "ml-model/ecg-pipeline-model"

  // ==========================================
  // PERCORSI DELTA LAKE (Storage Streaming)
  // ==========================================
  val DELTA_PREDICTIONS_PATH = "spark-warehouse/ecg_predictions"
  val STREAMING_CHECKPOINT_PATH = "spark-warehouse/checkpoints/ecg_predictions"
}