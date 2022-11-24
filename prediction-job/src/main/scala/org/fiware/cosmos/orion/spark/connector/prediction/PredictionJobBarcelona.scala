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
import org.apache.spark.sql.types.{StringType, DoubleType, StructField, StructType}
import scala.io.Source
//parámetro de la clase
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.fiware.cosmos.orion.spark.connector.NgsiEventLD
//devuelve el método
import org.apache.spark.streaming.dstream.DStream

import org.mongodb.scala.model.Filters.{equal, gt, and}
import com.mongodb.client.MongoClients
import com.mongodb.client.model.Sorts

import org.fiware.cosmos.orion.spark.connector.prediction.PredictionResponse

case class PredictionRequestBarcelona(id_estacion: Int, Ultima_medicion: Int, Diezhora_anterior: Int, Seishora_anterior: Int, Nuevehora_anterior: Int, variacion_estaciones: Double, dia: Int, hora: Int, num_mes: Int, socketId: String, predictionId: String, ciudad: String)

class PredictionJobBarcelona (eventStream: ReceiverInputDStream[NgsiEventLD]){

    def predict( MONGO_USERNAME: String, MONGO_PASSWORD: String, spark: SparkSession): DStream[PredictionResponse] = {

        val BASE_PATH = "./prediction-job"
        val modelBarcelona = PipelineModel.load(BASE_PATH+"/model/barcelona")

        def readFile(filename: String): Seq[String] = {
            val bufferedSource = Source.fromFile(filename)
            val lines = (for (line <- bufferedSource.getLines()) yield line).toList
            bufferedSource.close
            return lines
        }

        val variationStationsBarcelona = readFile("./prediction-job/array-load.txt")
        val dateTimeFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
        dateTimeFormatter.setTimeZone(TimeZone.getTimeZone("UTC"))

        val processedDataStream = eventStream 
            .flatMap(event => event.entities)
            .map(ent => {
                println(s"ENTITY RECEIVED: $ent")
                var idStation = ent.attrs("idStation")("value").toString.toInt
                var nombreCiudad = ent.attrs("ciudad")("value").toString
                var hour = ent.attrs("hour")("value").toString.toInt
                var weekday = ent.attrs("weekday")("value").toString.toInt
                var socketId = ent.attrs("socketId")("value").toString
                var predictionId = ent.attrs("predictionId")("value").toString
                var num = (idStation.toInt - 1 )
                var idVariationStation = num * 24 + hour
                var month = ent.attrs("month")("value").toString.toInt
                var dateNineHoursBefore = dateTimeFormatter.format(new Date(System.currentTimeMillis() - 3600 * 1000 *9))
                var dateSixHoursBefore = dateTimeFormatter.format(new Date(System.currentTimeMillis() - 3600 * 1000 *6))
                var lastMeasure: Int = 0
                var sixHoursAgoMeasure: Int = 0
                var nineHoursAgoMeasure: Int = 0
                var variationStation: Double = 0

                val mongoUri = s"mongodb://${MONGO_USERNAME}:${MONGO_PASSWORD}@mongo:27017/bikes_barcelona.historical?authSource=admin"
                val mongoClient = MongoClients.create(mongoUri)
                val collection = mongoClient.getDatabase("bikes_barcelona").getCollection("historical")
                val filter1 = and(gt("update_date", dateNineHoursBefore), equal("station_id", idStation.toString))
                val filter2 = and(gt("update_date", dateSixHoursBefore), equal("station_id", idStation.toString))
                val docs1 = collection.find(filter1)
                val docs2 = collection.find(filter2)
                variationStation = variationStationsBarcelona(idVariationStation).toString.toDouble
                lastMeasure = docs1.sort(Sorts.descending("update_date")).first().getString("num_bikes_available").toInt
                nineHoursAgoMeasure = docs1.sort(Sorts.ascending("update_date")).first().getString("num_bikes_available").toInt
                sixHoursAgoMeasure = docs2.sort(Sorts.ascending("update_date")).first().getString("num_bikes_available").toInt

                PredictionRequestBarcelona(idStation, lastMeasure, 0, sixHoursAgoMeasure, nineHoursAgoMeasure, variationStation, weekday, hour, month, socketId, predictionId, nombreCiudad)
        
            })    


            // Feed each entity into the prediction model
            val predictionDataStream = processedDataStream
            .transform(rdd => {
                val df = spark.createDataFrame(rdd)

                val predictions = modelBarcelona
                .transform(df)
                .select("socketId","predictionId", "prediction", "id_estacion", "dia", "hora", "num_mes")

                predictions.toJavaRDD
        
            })
            .map(pred=> PredictionResponse(
                pred.get(0).toString,
                pred.get(1).toString,
                pred.get(2).toString.toFloat.round,
                pred.get(3).toString.toInt,
                pred.get(4).toString.toInt,
                pred.get(5).toString.toInt,
                pred.get(6).toString.toInt
            )
            )

            return predictionDataStream
        }
}