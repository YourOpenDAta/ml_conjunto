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

class PredictionJobBarcelona extends Serializable{

    def readFile(filename: String): Seq[String] = {
      val bufferedSource = Source.fromFile(filename)
      val lines = (for (line <- bufferedSource.getLines()) yield line).toList
      bufferedSource.close
      return lines
    }

    val variationStationsBarcelona = readFile("./prediction-job/csv/array-load-bcn.txt")
    val BASE_PATH = "./prediction-job"
    val dateTimeFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    val modelBarcelona = PipelineModel.load(BASE_PATH+"/model/barcelona") 
    var mongoIsNull = false

    def request (ent: EntityLD, MONGO_USERNAME: String, MONGO_PASSWORD: String, nombreCiudad: String): PredictionRequest  = {
        dateTimeFormatter.setTimeZone(TimeZone.getTimeZone("UTC"))
        val idStation = ent.attrs("idStation")("value").toString
        val hour = ent.attrs("hour")("value").toString.toInt
        val weekday = ent.attrs("weekday")("value").toString.toInt
        val socketId = ent.attrs("socketId")("value").toString
        val predictionId = ent.attrs("predictionId")("value").toString
        val month = ent.attrs("month")("value").toString.toInt
        if (idStation.toInt > 519 || idStation.toInt < 1 || hour > 23 || hour < 0 || weekday > 7 || weekday < 1 || month > 12 || month < 1){
          mongoIsNull = true
          println("Some of the values introduced in the request are incorrect.")
        }
        val dateNineHoursBefore = dateTimeFormatter.format(new Date(System.currentTimeMillis() - 3600 * 1000 *9))
        val dateSixHoursBefore = dateTimeFormatter.format(new Date(System.currentTimeMillis() - 3600 * 1000 *6))

        var lastMeasure: Int = 5
        var sixHoursAgoMeasure: Int = 6
        var nineHoursAgoMeasure: Int = 7
        var tenHoursAgoMeasure: Int = 8
        
        val num = (idStation.toInt - 1 )
        val idVariationStation = num * 24 + hour
        val variationStation: Double = variationStationsBarcelona(idVariationStation).toString.toDouble
        
        val mongoUri = s"mongodb://${MONGO_USERNAME}:${MONGO_PASSWORD}@mongo:27017/bikes_barcelona.historical?authSource=admin"
        val mongoClient = MongoClients.create(mongoUri);
        val collection = mongoClient.getDatabase("bikes_barcelona").getCollection("historical")
        val filter1 = and(gt("update_date", dateNineHoursBefore), equal("station_id", idStation.toString))
        val filter2 = and(gt("update_date", dateSixHoursBefore), equal("station_id", idStation.toString))
        val docs1 = collection.find(filter1)
        val docs2 = collection.find(filter2)
          mongoIsNull = true
          
        try {
          lastMeasure = docs1.sort(Sorts.descending("update_date")).first().getString("num_bikes_available").toInt
          nineHoursAgoMeasure = docs1.sort(Sorts.ascending("update_date")).first().getString("num_bikes_available").toInt
          sixHoursAgoMeasure = docs2.sort(Sorts.ascending("update_date")).first().getString("num_bikes_available").toInt
          mongoIsNull = false
        } catch {
          case a: java.lang.NullPointerException => {
            println("NullPointerException: MongoDB is not ready, check if you started both mongo and nifi containers")
            mongoIsNull = true
          }
        }
        
         return PredictionRequest(idStation, lastMeasure, tenHoursAgoMeasure, sixHoursAgoMeasure, nineHoursAgoMeasure, variationStation, weekday, hour, month, socketId, predictionId, nombreCiudad)
         
    }

    def transform (rdd: RDD[PredictionRequest], spark: SparkSession): JavaRDD[Row] = {
      val df = spark.createDataFrame(rdd)
      val df1 = df.withColumn("id_estacion", col("id_estacion").cast(IntegerType))
      val predictions = modelBarcelona
      .transform(df1)
      .select("socketId","predictionId", "prediction", "id_estacion", "dia", "hora", "num_mes")
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
          pred.get(6).toString.toInt
        )
      } else {
        return PredictionResponse(
          pred.get(0).toString,
          pred.get(1).toString,
          pred.get(2).toString.toFloat.round,
          pred.get(3).toString,
          pred.get(4).toString.toInt,
          pred.get(5).toString.toInt,
          pred.get(6).toString.toInt
        )
      }
    } 
   } 