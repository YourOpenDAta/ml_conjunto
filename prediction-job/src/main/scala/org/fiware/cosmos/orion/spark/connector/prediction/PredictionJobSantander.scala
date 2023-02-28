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
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.fiware.cosmos.orion.spark.connector.NgsiEventLD
import org.apache.spark.streaming.dstream.DStream
import org.fiware.cosmos.orion.spark.connector.EntityLD
import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.Row

import org.mongodb.scala.model.Filters.{equal, gt, and}
import com.mongodb.client.MongoClients
import com.mongodb.client.model.Sorts

import org.fiware.cosmos.orion.spark.connector.prediction.PredictionResponse
import org.fiware.cosmos.orion.spark.connector.prediction.PredictionRequest

import java.io.Serializable 

import scala.util.{Try,Success,Failure}

class PredictionJobSantander extends Serializable {

  def readFile(filename: String): Seq[String] = {
    val bufferedSource = Source.fromFile(filename)
    val lines = (for (line <- bufferedSource.getLines()) yield line).toList
    bufferedSource.close
    return lines
  }

  val BASE_PATH = "./prediction-job"    
  val modelSantander = PipelineModel.load(BASE_PATH+"/model/santander")
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
      if (idStation.toInt > 17 || idStation.toInt < 1 || hour > 23 || hour < 0 || weekday > 7 || weekday < 1 || month > 12 || month < 1){
        mongoIsNull = true
        println("Some of the values introduced in the request are incorrect.")
      }
      val dateFifteenHoursBefore = dateTimeFormatter.format(new Date(System.currentTimeMillis() - 3600 * 1000 *15))
      val dateTenHoursBefore = dateTimeFormatter.format(new Date(System.currentTimeMillis() - 3600 * 1000 *10))
      val dateFiveHoursBefore = dateTimeFormatter.format(new Date(System.currentTimeMillis() - 3600 * 1000 *5))

      var lastMeasure: Int = 0
      var fiveHoursAgoMeasure: Int = 0
      var tenHoursAgoMeasure: Int = 0
      var fifteenHoursAgoMeasure: Int = 0
      
      val mongoUri = s"mongodb://${MONGO_USERNAME}:${MONGO_PASSWORD}@mongo:27017/bikes_santander.historical?authSource=admin"
      val mongoClient = MongoClients.create(mongoUri);
      val collection = mongoClient.getDatabase("bikes_santander").getCollection("historical")
      val filter1 = and(gt("update_date", dateFifteenHoursBefore), equal("dc:identifier", idStation.toString))
      val filter2 = and(gt("update_date", dateTenHoursBefore), equal("dc:identifier", idStation.toString))
      val filter3 = and(gt("update_date", dateFiveHoursBefore), equal("dc:identifier", idStation.toString))
      val docs1 = collection.find(filter1)
      val docs2 = collection.find(filter2)
      val docs3 = collection.find(filter3)
      try {
      lastMeasure = docs1.sort(Sorts.descending("update_date")).first().getString("ayto:bicicletas_libres").toInt
      fifteenHoursAgoMeasure = docs1.sort(Sorts.ascending("update_date")).first().getString("ayto:bicicletas_libres").toInt
      tenHoursAgoMeasure = docs2.sort(Sorts.ascending("update_date")).first().getString("ayto:bicicletas_libres").toInt
      fiveHoursAgoMeasure = docs3.sort(Sorts.ascending("update_date")).first().getString("ayto:bicicletas_libres").toInt
      mongoIsNull = false
      } catch {
        case a: java.lang.NullPointerException => {
          println("NullPointerException: MongoDB is not ready, check if you started both mongo and nifi containers")
          mongoIsNull = true
        }
      }
          
      return PredictionRequest(idStation, lastMeasure, fiveHoursAgoMeasure, tenHoursAgoMeasure, fifteenHoursAgoMeasure, weekday, hour, month, socketId, predictionId, cityName, 1)
  }

  def transform (rdd: RDD[PredictionRequest], numberOfIterations: Int, spark: SparkSession): JavaRDD[Row] = {
    
    val df = spark.createDataFrame(rdd)
    val df2 = df.withColumn("id_station", col("id_station").cast(IntegerType))
    val df3 = df2
                    .withColumnRenamed("last_measure", "five_before")
                    .withColumnRenamed("two_last_measure", "ten_before")
                    .withColumnRenamed("three_last_measure", "fifteen_before")
                    .withColumnRenamed("four_last_measure", "twenty_before")
                  
    val predictions = modelSantander
      .transform(df3)
      .select("socketId","predictionId", "prediction", "id_station", "day", "hour", "month")

    val predictionsFinal = predictions.withColumn("city", lit("Santander"))
 
    if (numberOfIterations > 5 || numberOfIterations <= 1) {
      // only predictions in the following 24 hours
      return predictionsFinal.toJavaRDD
    } else {
      val previousPredictionRequest = rdd.first()
      val nextPredictionRequest = PredictionRequest(
        previousPredictionRequest.id_station,
        predictions.head().get(2).toString.toFloat.round,
        previousPredictionRequest.last_measure,
        previousPredictionRequest.two_last_measure,
        previousPredictionRequest.three_last_measure,
        previousPredictionRequest.day,
        previousPredictionRequest.hour,
        previousPredictionRequest.month,
        previousPredictionRequest.socketId,
        previousPredictionRequest.predictionId,
        previousPredictionRequest.city,
        numberOfIterations-1
      )
        return transform(spark.sparkContext.parallelize(List(nextPredictionRequest)), numberOfIterations-1, spark)

    }

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
        "Santander"
      )
    } else {
      return PredictionResponse(
        pred.get(0).toString,
        pred.get(1).toString,
        pred.get(2).toString.toFloat.round,
        pred.get(3).toString,
        pred.get(4).toString.toInt,
        pred.get(5).toString.toInt,
        pred.get(6).toString.toInt,
        "Santander"
      )
    }
  }
}