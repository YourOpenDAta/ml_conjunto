package org.fiware.cosmos.orion.spark.connector.prediction

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer, StringIndexer, OneHotEncoder}
import org.apache.spark.ml.regression._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StringType, IntegerType, DoubleType, StructField, StructType}

object TrainingJobSantander {

  def main(args: Array[String]): Unit = {
    train().write.overwrite().save("./prediction-job/model/santander")

  }
  
  def train() = {
    val schema = StructType(
      Array(StructField("available_bikes", IntegerType),
            StructField("modified", StringType),      
            StructField("id_station", IntegerType),
            StructField("five_before", IntegerType),
            StructField("ten_before", IntegerType),
            StructField("fifteen_before", IntegerType),
            StructField("twenty_before", IntegerType)            
      ))
    val spark = SparkSession
      .builder
      .appName("TrainingBikeSantander")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    // convert to dataframe
    val data = spark.read.format("csv")
      .schema(schema)
      .option("header", "true")
      .option("delimiter", ",")
      .load("./prediction-job/csv/santander_bike.csv")

              
    val vectorAssembler  = new VectorAssembler()
      .setInputCols(Array("id_station", "five_before","ten_before","fifteen_before","twenty_before"))
      .setOutputCol("features")
    


    val rfc = new DecisionTreeRegressor()
      .setMaxDepth(8)
      .setLabelCol("available_bikes")
      .setFeaturesCol("features")

    val pipeline = new Pipeline().setStages(Array(vectorAssembler,rfc))
    val Array(trainingData,testData) = data.randomSplit(Array(0.8,0.2))
    val model = pipeline.fit(trainingData)
    val predictions = model.transform(testData)

    predictions.select("prediction", "available_bikes", "id_station", "five_before","ten_before","fifteen_before","twenty_before").show(10)

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