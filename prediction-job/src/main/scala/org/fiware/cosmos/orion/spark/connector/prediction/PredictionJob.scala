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

import java.util.{Date, TimeZone}

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

case class PredictionRequest(id_station: String, last_measure: Int, two_last_measure: Int, three_last_measure: Int, four_last_measure: Int, day: Int, hour: Int, month: Int, socketId: String, predictionId: String, city: String, numberOfIterations: Int)

object PredictionJob {

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
    
    println("STARTING PREDICTION SYSTEM....")
    
    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

    val eventStream = ssc.receiverStream(new NGSILDReceiver(9002))

    var pred1 = new PredictionJobBarcelona()
    var pred2 = new PredictionJobSantander()
    var pred3 = new PredictionJobMalaga()
    val processedDataStream = eventStream
      .flatMap(event => event.entities)
      .map(ent => {
          println(s"ENTITY RECEIVED: $ent")
          val cityName = ent.attrs("city")("value").toString      
          if (cityName == "Barcelona") {
             pred1.request(ent, MONGO_USERNAME, MONGO_PASSWORD, cityName)
           } else if (cityName == "Malaga"){
             pred3.request(ent, MONGO_USERNAME, MONGO_PASSWORD, cityName)
           } else if (cityName == "Santander") {
             pred2.request(ent, MONGO_USERNAME, MONGO_PASSWORD, cityName)
           } else {
            pred3.request(ent, MONGO_USERNAME, MONGO_PASSWORD, cityName)
           }
          
        })
    
    // Feed each entity into the prediction model
    val predictionDataStream = processedDataStream
      .transform(rdd => {
           val df = rdd.toDF
           val hoursNow = new Date(System.currentTimeMillis()).getHours().toInt
           var city = "Santander"
           var hour: Int = 0
           try {
            city = df.head().get(10).toString
            hour = df.head.get(6).toString.toInt
           } catch {
            case _ => city = "Other"
           }
          
           if (city == "Barcelona") {
             val numberOfIterations = (if (hoursNow <= hour) hour - hoursNow else hour + 24 - hoursNow)/3.toInt + 1
             pred1.transform(rdd, numberOfIterations, spark)
           } else if (city == "Malaga"){
             pred3.transform(rdd, spark)
           } else if (city == "Santander") {
            val numberOfIterations = (if (hoursNow <= hour) hour - hoursNow else hour + 24 - hoursNow)/5.toInt + 1
            pred2.transform(rdd, numberOfIterations, spark)
           } else {
            pred2.transform(rdd, 1, spark)
           }
      })
      .map(pred=> {
          val city = pred.get(7).toString
           if (city == "Barcelona") {
             pred1.response(pred)
           } else if (city == "Malaga"){
             pred3.response(pred)
           } else if (city == "Santander") {
             pred2.response(pred)
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