package prediction

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.fiware.cosmos.orion.spark.connector.{ContentType, HTTPMethod, NGSILDReceiver, OrionSink, OrionSinkObject}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{VectorAssembler}
import org.apache.spark.ml.classification.{RandomForestClassificationModel}
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
import org.fiware.cosmos.orion.spark.connector.prediction.PredictionResponse

//cambiado por Pablo y luego por cris
case class PredictionRequestMalaga(name: String, weekday: Int, hour: Int, month: Int, socketId: String, predictionId: String)

class PredictionJobMalaga(eventStream: ReceiverInputDStream[NgsiEventLD]) {

    def predict(spark: SparkSession): DStream[PredictionResponse] = {

        val BASE_PATH = "./prediction-job"
        // Load model
        val model = PipelineModel.load(BASE_PATH+"/model/malaga")

        // Process event stream to get updated entities
        val processedDataStream = eventStream
        .flatMap(event => event.entities)
        .map(ent => {
        println(s"ENTITY RECEIVED: $ent")
        val month = ent.attrs("month")("value").toString.toInt
        val name = ent.attrs("idStation")("value").toString
        val hour = ent.attrs("hour")("value").toString.toInt
        val weekday = ent.attrs("weekday")("value").toString.toInt
        val socketId = ent.attrs("socketId")("value").toString
        val predictionId = ent.attrs("predictionId")("value").toString
        PredictionRequestMalaga(name, weekday, hour, month, socketId, predictionId)
      })

        // Feed each entity into the prediction model
        val predictionDataStream = processedDataStream
        .transform(rdd => {
        val df = spark.createDataFrame(rdd)
        val predictions = model
        .transform(df)
        .select("socketId","predictionId", "prediction", "name", "weekday", "hour", "month")

        predictions.toJavaRDD
    })
      .map(pred=> PredictionResponse(
        pred.get(0).toString,
        pred.get(1).toString,
        pred.get(2).toString.toFloat.round * 10,
        0,
        pred.get(4).toString.toInt,
        pred.get(5).toString.toInt,
        pred.get(6).toString.toInt,
        pred.get(3).toString
      )
    )

    return predictionDataStream
    
    }
}
