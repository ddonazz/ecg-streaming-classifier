# Progetto di Tesi: ECG Streaming Classifier (Big Data Architecture)

## 📌 Descrizione del Progetto
Questo progetto implementa una pipeline architetturale Big Data all'avanguardia per la classificazione in tempo reale di aritmie cardiache tramite segnali ECG.
L'obiettivo è dimostrare come le moderne tecnologie di elaborazione distribuita e message brokering possano supportare scenari di Telemedicina (IoT/Edge Computing), elaborando flussi di dati vitali ad altissima frequenza.

Rispetto agli approcci tradizionali basati su attori (es. Akka), questa architettura abbraccia il paradigma **Data Lakehouse** e lo **Stream Processing**, garantendo scalabilità orizzontale, fault-tolerance e disaccoppiamento dei servizi.

---

## 🛠️ Stack Tecnologico
* **Linguaggio:** Scala 2.13
* **Motore di Calcolo:** Apache Spark 4.1.1
    * *Spark SQL:* Per la manipolazione dei DataFrame.
    * *Spark MLlib:* Per il training del modello di Machine Learning (Random Forest).
    * *Spark Structured Streaming:* Per l'inferenza in tempo reale sui flussi di dati.
* **Message Broker:** Apache Kafka (Bitnami Image in modalità KRaft, via Docker)
* **Storage Level:** Delta Lake 4.1.0 (Formato Parquet transazionale ACID)
* **Build Tool:** SBT 

---

## 📊 Il Dataset
Si utilizza l'**ECG Arrhythmia Classification Dataset** (derivato dal MIT-BIH via PhysioNet).
A differenza dei segnali grezzi continui, questo dataset fornisce **feature mediche pre-estratte** a bordo sensore (Edge AI approach), riducendo il payload di rete.

* **Righe:** ~1 Milione di battiti cardiaci singoli.
* **Features:** 34 misurazioni cliniche per battito (Intervalli RR, QRS duration, ampiezze dei picchi).
* **Label (Target):** 5 classi di patologie (N = Normale, V = Ventricolare, S = Sopraventricolare, F = Fusione, Q = Sconosciuto).

---

## 🏗️ Architettura e Flusso di Esecuzione

L'architettura è suddivisa in tre fasi logiche, ognuna gestita da una classe specifica.

### Fase 1: Batch Training (Addestramento Offline)
**Classe:** `it.utiu.thesis.batch.EcgModelTrainer`

1.  **Ingestion:** Legge storicamente i file CSV contenenti il milione di battiti.
2.  **Data Cleaning:** Elimina i record corrotti o con valori mancanti (`NULL`).
3.  **Feature Engineering:** * Usa `StringIndexer` per convertire la colonna patologia in valori numerici (`label`).
    * Usa `VectorAssembler` per unire le 34 misurazioni in un array matematico (`features`).
4.  **Training:** Divide i dati (80% Train, 20% Test) e addestra un algoritmo **Random Forest Classifier**.
5.  **Persistenza:** Valuta l'accuratezza e salva il modello binario serializzato su disco (nella cartella `ml-model/`).

### Fase 2: Data Ingestion & Simulation (IoT Edge Device)
**Classe:** `it.utiu.thesis.streaming.EcgPatientSimulator` (Producer Kafka)

Non avendo pazienti reali connessi, questa classe funge da simulatore hardware (es. un Holter o uno smartwatch).
1.  Legge un set di dati ECG di test.
2.  Itera sui record inserendo un micro-delay (es. 1 secondo) per simulare lo scorrere del tempo.
3.  Converte i dati della riga corrente in un payload JSON.
4.  *(Opzionale)*: Applica un leggerissimo rumore gaussiano ai dati per simulare misurazioni infinite senza inviare doppioni.
5.  Invia il JSON al **Topic Kafka** denominato `ecg-input-stream`.

### Fase 3: Real-Time Inference (Cloud Streaming)
**Classe:** `it.utiu.thesis.streaming.EcgStreamingPredictor` (Consumer Kafka & Spark)

È il cuore del sistema Big Data, progettato per rimanere sempre acceso (H24).
1.  All'avvio, carica in memoria il modello Random Forest precedentemente salvato.
2.  Si connette ad Apache Kafka tramite le API di **Spark Structured Streaming**.
3.  Si mette in ascolto continuo sul topic `ecg-input-stream`.
4.  Ogni volta che arriva un micro-batch di nuovi battiti cardiaci (JSON):
    * Esegue il parsing del JSON ricreando lo schema tabellare.
    * Applica la pipeline ML (VectorAssembler + RandomForest).
    * Ottiene la predizione (il battito è sano o infartuato?).
5.  **Sink:** Scrive il risultato in tempo reale sulla console e lo salva su disco usando il formato **Delta Lake** per future analisi o dashboard.

---

## 📂 Struttura dei Package

```text
ecg-streaming-classifier/
├── build.sbt                  # Dipendenze e setup del progetto
├── docker-compose.yml         # Configurazione infrastruttura Kafka
├── .gitignore                 # File ignorati da Git
├── ml-model/                  # [Generata] Contiene il modello ML addestrato
├── spark-warehouse/           # [Generata] Metadati di Spark e Delta Lake
└── src/main/scala/it/utiu/thesis/
    ├── batch/
    │   └── EcgModelTrainer.scala       # Job 1: Addestramento
    ├── streaming/
    │   ├── EcgPatientSimulator.scala   # Job 2: Simulatore Sensore (Kafka Producer)
    │   └── EcgStreamingPredictor.scala # Job 3: Inferenza Real-Time
    └── utils/
        └── Config.scala                # File opzionale per costanti (IP, Topic)