package it.utiu.thesis
package streaming

import utils.Config

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, struct, to_json}

import java.util.Properties

object EcgPatientSimulator {
  def main(args: Array[String]): Unit = {
    println("Avvio del Simulatore Paziente ECG (Kafka Producer)...")

    val spark = SparkSession.builder()
      .appName("EcgPatientSimulator")
      .master(Config.SPARK_MASTER)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val datasetPath = Config.DATASET_PATH

    println(s"Lettura dei dati da: $datasetPath")
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(datasetPath)
      .na.drop()
      .limit(1000)

    println("Conversione dei dati in formato JSON...")

    val jsonRecords = df.select(to_json(struct(col("*"))).alias("json_payload"))
      .select("json_payload")
      .as[String]
      .collect()

    // spark.stop()

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.KAFKA_BOOTSTRAP_SERVERS)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val topic = Config.KAFKA_TOPIC

    println(s"Inizio invio di ${jsonRecords.length} battiti cardiaci al topic Kafka '$topic'...")
    println("Premi CTRL+C per interrompere la simulazione.\n")

    for ((jsonPayload, index) <- jsonRecords.zipWithIndex) {
      val record = new ProducerRecord[String, String](topic, s"beat-$index", jsonPayload)
      producer.send(record)
      println(s"[INVIATO] Battito #$index -> $jsonPayload")
      Thread.sleep(1000)
    }

    producer.flush()
    producer.close()
    println("Simulazione terminata.")
  }
}