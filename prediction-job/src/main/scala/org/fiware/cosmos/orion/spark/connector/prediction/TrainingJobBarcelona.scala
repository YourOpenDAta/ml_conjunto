package org.fiware.cosmos.orion.spark.connector.prediction

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer, StringIndexer, OneHotEncoder}
import org.apache.spark.ml.regression._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, DoubleType, StructField, StructType}

object TrainingJobBarcelona {

  def main(args: Array[String]): Unit = {
    train().write.overwrite().save("./prediction-job/model/barcelona")

  }
  
  def train() = {
    val schema = StructType(
      Array(StructField("available_bikes", IntegerType),
            StructField("id_station", IntegerType),
            StructField("hour", IntegerType),
            StructField("three_before", IntegerType),
            StructField("six_before", IntegerType),
            StructField("nine_before", IntegerType),
            StructField("day", IntegerType),
            StructField("blank", IntegerType),
            StructField("comp", IntegerType)
      ))
    val spark = SparkSession
      .builder
      .appName("TrainingBikeBarcelona")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    // convert to dataframe
    val data = spark.read.format("csv")
      .schema(schema)
      .option("header", "true")
      .option("delimiter", ",")
      .load("./prediction-job/csv/barcelona_bike.csv")

              
    val vectorAssembler  = new VectorAssembler()
      .setInputCols(Array("id_station", "three_before", "six_before","nine_before","hour"))
      .setOutputCol("features")


    val rfc = new DecisionTreeRegressor()
      .setMaxDepth(10)
      .setLabelCol("available_bikes")
      .setFeaturesCol("features")

    val pipeline = new Pipeline().setStages(Array(vectorAssembler,rfc))
    val Array(trainingData,testData) = data.randomSplit(Array(0.8,0.2))
    val model = pipeline.fit(trainingData)
    val predictions = model.transform(testData)

    predictions.select("prediction","available_bikes", "id_station", "three_before","six_before", "nine_before").show(10)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("available_bikes")
      .setPredictionCol("prediction")

    val rmse = evaluator.setMetricName("rmse").evaluate(predictions)
    println(s"rmse = $rmse")

    val mae = evaluator.setMetricName("mae").evaluate(predictions)
    println(s"mae = $mae")

    val r2 = evaluator.setMetricName("r2").evaluate(predictions)
    println(s"r2 = $r2")


    model
  }
}