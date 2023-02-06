package org.fiware.cosmos.orion.spark.connector.prediction

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.fiware.cosmos.orion.spark.connector.{ContentType, HTTPMethod, NGSILDReceiver, OrionSink, OrionSinkObject}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{VectorAssembler}
import org.apache.spark.ml.regression.{DecisionTreeRegressor}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.types.{StringType, DoubleType, StructField, StructType, IntegerType}
import scala.io.Source
import org.apache.spark.sql.functions.col

import org.mongodb.scala.model.Filters.{equal, gt, and}
import com.mongodb.client.MongoClients
import com.mongodb.client.model.Sorts

import prediction.PredictionJobSantander
import prediction.PredictionJobBarcelona
import prediction.PredictionJobMalaga
import org.apache.spark.streaming.dstream.DStream

case class PredictionResponse(socketId: String, predictionId: String, predictionValue: Int, idStation: String, weekday: Int, hour: Int, month: Int, city: String) {
  override def toString :String = s"""{
  "socketId": { "value": "${socketId}", "type": "Property"},
  "predictionId": { "value":"${predictionId}", "type": "Property"},
  "predictionValue": { "value":${predictionValue}, "type": "Property"},
  "idStation": { "value":"${idStation}", "type": "Property"},
  "weekday": { "value":${weekday}, "type": "Property"},
  "hour": { "value": ${hour}, "type": "Property"},
  "month": { "value": ${month}, "type": "Property"},
  "city": { "value": "${city}", "type": "Property"}
  }""".trim()
}

case class PredictionRequest(id_estacion: String, Ultima_medicion: Int, Diezhora_anterior: Int, Seishora_anterior: Int, Nuevehora_anterior: Int, variacion_estaciones: Double, dia: Int, hora: Int, num_mes: Int, socketId: String, predictionId: String, city: String)

object PredictionJob {

  var cityName = "Santander"
  final val URL_CB = "http://orion:1026/ngsi-ld/v1/entities/urn:ngsi-ld:ResPrediction1/attrs"
  final val CONTENT_TYPE = ContentType.JSON
  final val METHOD = HTTPMethod.PATCH
  final val MONGO_USERNAME = System.getenv("MONGO_USERNAME")
  final val MONGO_PASSWORD = System.getenv("MONGO_PASSWORD")

    def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("PredictingBikeConjunto")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")
    
    println("STARTING BIKE CONJUNTO....")
    
    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

    val eventStream = ssc.receiverStream(new NGSILDReceiver(9002))

    var cityName: String = ""
    var pred1 = new PredictionJobBarcelona()
    var pred2 = new PredictionJobSantander()
    var pred3 = new PredictionJobMalaga()
    val processedDataStream = eventStream
      .flatMap(event => event.entities)
      .map(ent => {
          println(s"ENTITY RECEIVED: $ent")
          cityName = ent.attrs("city")("value").toString
          if (cityName == "Barcelona") {
             pred1.request(ent, MONGO_USERNAME, MONGO_PASSWORD, cityName)
           } else if (cityName == "Malaga"){
             pred3.request(ent, MONGO_USERNAME, MONGO_PASSWORD, cityName)
           } else {
             pred2.request(ent, MONGO_USERNAME, MONGO_PASSWORD, cityName)
           }
          
        })
    
    // Feed each entity into the prediction model
    val predictionDataStream = processedDataStream
      .transform(rdd => {
           if (cityName == "Barcelona") {
             pred1.transform(rdd, spark)
           } else if (cityName == "Malaga"){
             pred3.transform(rdd, spark)
           } else {
             pred2.transform(rdd, spark)
           }
      })
      .map(pred=> {
           if (cityName == "Barcelona") {
             pred1.response(pred)
           } else if (cityName == "Malaga"){
             pred3.response(pred)
           } else {
             pred2.response(pred)
           }     
      })
      
    // Convert the output to an OrionSinkObject and send to Context Broker
    val sinkDataStream = predictionDataStream
      .map(res => OrionSinkObject(res.toString, URL_CB, CONTENT_TYPE, METHOD))

    // Add Orion Sink
    OrionSink.addSink(sinkDataStream)
    ssc.start()
    ssc.awaitTermination()
  
}
}