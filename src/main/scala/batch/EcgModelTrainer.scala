package it.utiu.thesis
package batch

import utils.Config

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession

object EcgModelTrainer {
  def main(args: Array[String]): Unit = {
    println("Inizializzazione Spark per classificazione ECG...")

    val spark = SparkSession.builder()
      .appName("EcgModelTrainer")
      .master(Config.SPARK_MASTER)
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.codegen.wholeStage", "false")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println(s"Spark avviato con successo! Versione: ${spark.version}")

    val datasetPath = Config.DATASET_PATH
    println(s"Leggendo i dataset da: $datasetPath")
    val rawDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(datasetPath)

    val cleanDF = rawDF.na.drop()

    println(s"Numero totale di battiti caricati e puliti: ${cleanDF.count()}")

    println("Tipi di patologie nel dataset:")
    cleanDF.groupBy("type").count().show()

    println("Inizio Data Preparation e setup della Pipeline ML...")

    val featureCols = cleanDF.columns.filter(colName =>
      colName != "record" && colName != "type" && colName != "label"
    )

    val Array(trainingData, testData) = cleanDF.randomSplit(Array(0.8, 0.2), seed = 42L)

    val indexer = new StringIndexer()
      .setInputCol("type")
      .setOutputCol("label")
      .setHandleInvalid("keep")

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
      .setHandleInvalid("keep")

    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(50)
      .setMaxDepth(10)

    val pipeline = new Pipeline().setStages(Array(indexer, assembler, rf))

    println("Addestramento della Pipeline in corso (potrebbe richiedere qualche minuto)...")
    val pipelineModel = pipeline.fit(trainingData)

    println("Valutazione del modello sul Test Set...")
    val predictions = pipelineModel.transform(testData)
    predictions.select("record", "type", "label", "prediction", "probability").show(10, truncate = false)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)
    println(s"=== ACCURATEZZA DEL MODELLO: ${accuracy * 100}%.2f ===")

    val modelPath = Config.ML_MODEL_PATH
    println(s"Salvataggio della Pipeline completa su disco nella directory: $modelPath ...")
    pipelineModel.write.overwrite().save(modelPath)

    println("Job di Batch Training completato con successo!")
    spark.stop()
  }
}