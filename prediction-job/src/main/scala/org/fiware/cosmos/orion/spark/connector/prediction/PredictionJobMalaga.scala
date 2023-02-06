package prediction
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.fiware.cosmos.orion.spark.connector.{ContentType, HTTPMethod, NGSILDReceiver, OrionSink, OrionSinkObject}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{VectorAssembler}
import org.apache.spark.ml.regression.{DecisionTreeRegressor}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.DataStreamReader
import java.util.{Date, TimeZone}
import java.text.SimpleDateFormat
import org.apache.spark.sql.types.{StringType, DoubleType, StructField, StructType, IntegerType}
import scala.io.Source
import org.apache.spark.sql.functions.col
//parámetro de la clase
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.fiware.cosmos.orion.spark.connector.NgsiEventLD
import org.fiware.cosmos.orion.spark.connector.EntityLD
import org.apache.spark.rdd.RDD
//devuelve el método
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.Row

import org.mongodb.scala.model.Filters.{equal, gt, and}
import com.mongodb.client.MongoClients
import com.mongodb.client.model.Sorts

import org.fiware.cosmos.orion.spark.connector.prediction.PredictionResponse
import org.fiware.cosmos.orion.spark.connector.prediction.PredictionRequest

import java.io.Serializable 

class PredictionJobMalaga extends Serializable{

  val BASE_PATH = "./prediction-job"    
  val modelMalaga = PipelineModel.load(BASE_PATH+"/model/malaga")
  val dateTimeFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
  var mongoIsNull = false

    def request (ent: EntityLD, MONGO_USERNAME: String, MONGO_PASSWORD: String, cityName: String): PredictionRequest  = {
        dateTimeFormatter.setTimeZone(TimeZone.getTimeZone("UTC"))
        val idStation = ent.attrs("idStation")("value").toString
        val hour = ent.attrs("hour")("value").toString.toInt
        val weekday = ent.attrs("weekday")("value").toString.toInt
        val socketId = ent.attrs("socketId")("value").toString
        val predictionId = ent.attrs("predictionId")("value").toString
        val month = ent.attrs("month")("value").toString.toInt
        if (idStation.toInt > 9 || idStation.toInt < 0 || hour > 23 || hour < 0 || weekday > 7 || weekday < 1 || month > 12 || month < 1){
          mongoIsNull = true
          println("Some of the values introduced in the request are incorrect.")
        }

        var lastMeasure: Int = 0
        var sixHoursAgoMeasure: Int = 0
        var nineHoursAgoMeasure: Int = 0
        var tenHoursAgoMeasure: Int = 0
        
        val variationStation: Double =  0.0

            
        return PredictionRequest(idStation, lastMeasure, tenHoursAgoMeasure, sixHoursAgoMeasure, nineHoursAgoMeasure, variationStation, weekday, hour, month, socketId, predictionId, cityName)
    }

    def transform (rdd: RDD[PredictionRequest], spark: SparkSession): JavaRDD[Row] = {
      val df = spark.createDataFrame(rdd)
      val df2 = df
                    .withColumnRenamed("id_estacion", "name")
                    .withColumnRenamed("hora", "hour")
                    .withColumnRenamed("dia", "weekday")
                    .withColumnRenamed("num_mes", "month")
                    .select("name", "hour", "weekday", "month", "socketId","predictionId")
        val predictions = modelMalaga
          .transform(df2)
          .select("socketId","predictionId", "prediction", "name", "weekday", "hour", "month")
          return predictions.toJavaRDD
    }

    def response (pred: Row): PredictionResponse = {
    if (mongoIsNull) {
      return PredictionResponse(
        pred.get(0).toString,
        pred.get(1).toString,
        -1,
        pred.get(3).toString,
        pred.get(4).toString.toInt,
        pred.get(5).toString.toInt,
        pred.get(6).toString.toInt,
        "Malaga"
      )
    } else {
      return PredictionResponse(
        pred.get(0).toString,
        pred.get(1).toString,
        pred.get(2).toString.toFloat.round * 10,
        pred.get(3).toString,
        pred.get(4).toString.toInt,
        pred.get(5).toString.toInt,
        pred.get(6).toString.toInt,
        "Malaga"
      )
    }
    }
}