package  org.fiware.cosmos.orion.spark.connector.prediction

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.fiware.cosmos.orion.spark.connector.{ContentType, HTTPMethod, NGSILDReceiver, OrionSink, OrionSinkObject}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{VectorAssembler}
import org.apache.spark.ml.regression.{DecisionTreeRegressor}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.DataStreamReader
import java.util.{Date, TimeZone}
import java.text.SimpleDateFormat
import org.apache.spark.sql.types.{StringType, DoubleType, StructField, StructType}
import scala.io.Source

import org.mongodb.scala.model.Filters.{equal, gt, and}
import com.mongodb.client.MongoClients
import com.mongodb.client.model.Sorts

import prediction.PredictionJobSantander
import prediction.PredictionJobBarcelona
import prediction.PredictionJobMalaga
import org.apache.spark.streaming.dstream.DStream

case class PredictionResponse(socketId: String, predictionId: String, predictionValue: Int, idStation: Int, weekday: Int, hour: Int, month: Int, name: String) {
    override def toString :String = s"""{
    "socketId": { "value": "${socketId}", "type": "Property"},
    "predictionId": { "value":"${predictionId}", "type": "Property"},
    "predictionValue": { "value":${predictionValue}, "type": "Property"},
    "idStation": { "value":"${idStation}", "type": "Property"},
    "weekday": { "value":${weekday}, "type": "Property"},
    "hour": { "value": ${hour}, "type": "Property"},
    "month": { "value": ${month}, "type": "Property"}
    }""".trim()
    }

object PredictionJob {

  var nombreCiudad = "Santander"
  final val URL_CB = "http://orion:1026/ngsi-ld/v1/entities/urn:ngsi-ld:ResBikePrediction1/attrs"
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
    
    // Process event stream to get updated entities
    val preprocessedDataStream = eventStream
      .flatMap(event => event.entities)
      .map(ent => {
        println(s"ENTITY RECEIVED: $ent")
        nombreCiudad = ent.attrs("ciudad")("value").toString
      })
    
    val event1 = new PredictionJobBarcelona(eventStream)
    val event2 = new PredictionJobSantander(eventStream)
    val event3 = new PredictionJobMalaga(eventStream)
    var predictionDataStream :  DStream[PredictionResponse] = event1.predict(MONGO_USERNAME, MONGO_PASSWORD, spark)

    if (nombreCiudad == "Barcelona"){
        predictionDataStream = event1.predict(MONGO_USERNAME, MONGO_PASSWORD, spark)
    } 
    
    if (nombreCiudad == "Santander"){
        predictionDataStream = event2.predict(MONGO_USERNAME, MONGO_PASSWORD, spark)
    }

    if (nombreCiudad == "Malaga") {
        predictionDataStream = event3.predict(spark)
    }

    // Convert the output to an OrionSinkObject and send to Context Broker
    val sinkDataStream = predictionDataStream
      .map(res => OrionSinkObject(res.toString, URL_CB, CONTENT_TYPE, METHOD))

    // Add Orion Sink
    OrionSink.addSink(sinkDataStream)
    ssc.start()
    ssc.awaitTermination()
  }
}

 