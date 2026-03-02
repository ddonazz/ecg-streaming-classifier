package it.utiu.thesis

import batch.EcgModelTrainer
import streaming.{EcgPatientSimulator, EcgStreamingPredictor}
import utils.Config

import java.io.File

object AppRunner {
  def main(args: Array[String]): Unit = {
    println("=====================================================")
    println("  🏥 ECG STREAMING CLASSIFIER - BIG DATA PIPELINE  ")
    println("=====================================================\n")

    // 1. Controllo e Addestramento Modello (Fase Batch)
    val modelDir = new File(Config.ML_MODEL_PATH)
    if (!modelDir.exists()) {
      println("[STEP 1] Modello ML non trovato. Avvio il Batch Training...")
      EcgModelTrainer.main(Array.empty)
    } else {
      println("[STEP 1] Modello ML già presente su disco. Salto l'addestramento.")
    }

    // 2. Avvio del Predictor Streaming in un Thread separato (Fase Consumer)
    println("\n[STEP 2] Avvio del motore di Streaming Inference (Background)...")
    val predictorThread = new Thread(() => {
      EcgStreamingPredictor.main(Array.empty)
    })

    // Settiamo il thread come demone, così se chiudiamo il programma principale si chiude anche lui
    predictorThread.setDaemon(true)
    predictorThread.start()

    // Attendiamo 15 secondi per assicurarci che Spark Streaming sia pronto e connesso a Kafka
    println("Attesa di 15 secondi per l'inizializzazione del nodo Spark Streaming...")
    Thread.sleep(15000)

    // 3. Avvio del Simulatore Paziente (Fase Producer)
    println("\n[STEP 3] Avvio del Simulatore Sensore IoT (Kafka Producer)...")
    EcgPatientSimulator.main(Array.empty)

    // Siccome il simulatore finisce dopo 1000 record, teniamo vivo il programma
    // per permettere allo streaming di finire di processare gli ultimi dati
    println("\n[INFO] Simulazione invio terminata. Il nodo di streaming rimane in ascolto...")
    predictorThread.join()
  }
}