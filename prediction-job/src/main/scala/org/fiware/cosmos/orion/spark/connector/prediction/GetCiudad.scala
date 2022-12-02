package prediction

//parámetro de la clase
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.fiware.cosmos.orion.spark.connector.NgsiEventLD

class GetCiudad(eventStream: ReceiverInputDStream[NgsiEventLD]){
    def getCity(): String = {
        var nombreCiudad: String = "Juan"

        eventStream 
            .flatMap(event => event.entities)
            .map(ent => {
                println(s"ENTITY RECEIVED GetCiudad: $ent")
                nombreCiudad = ent.attrs("ciudad")("value").toString
            })
        
        return nombreCiudad
    }
}