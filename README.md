# YODA - ML bike Barcelona y Santander integrados junto con Malaga Parking

* Barcelona: https://api.bsmsa.eu/ext/api/bsm/gbfs/v2/en/station_status
* Santander: http://datos.santander.es/resource/?ds=estado-estaciones-bicicletas&id=c798a2d2-bb27-452e-9a97-25d97b5c21ac&ft=JSON http://datos.santander.es/api/rest/datasets/tusbic_puestos_libres.json?items=17
* Malaga: https://datosabiertos.malaga.eu/dataset/ocupacion-aparcamientos-publicos-municipales https://datosabiertos.malaga.eu/dataset/ubicacion-de-aparcamientos-publicos-municipales

## Description

* Barcelona's endpoint returns the number of bikes available in the requested station
* Santander's endpoint returns the number of bikes available in the requested station
* Malaga's endpoint returns the percentage of occupancy

## Initial tasks

* Copy the `template.env` file into `.env` and configure it.
* Create ckan_yoda network
```shell
docker network create ckan_yoda
```


## Training

* Clone this project

* If the model already exists (`prediction-job/model`) just build de project packages
```shell
docker compose -f docker-compose.build.packages.yml up -d --build
```

* If the model does not exist: Build the packages and train the model
```shell
docker compose -f docker-compose.train.yml up -d --build
```
* View the spark deployment in http://localhost:4040/jobs/

* Test that the `prediction-job/model` folder is generated with the ML model

## Testing (Development)

* Run the whole scenario in dev
```shell
docker compose -f docker-compose.dev.yml up -d
```

* Initialize the nifi recollection (Nifi will recollect data every 30 minutes and will store only the last 24 hours of historical data):
  
  - Enter in the nifi interface: https://localhost:9090/nifi
  
  - Log in with the following credentials: 
      - username: root
      - password: pass1234567890
      
   - Upload the template stored in ./nifi/Barcelona_template.xml
   - Upload the template stored in ./nifi/Santander_template.xml
   
   - Run every component deployed by the template
   
* Create the prediction entities and the subscriptions:

  - Enter in the orion container
  
  ```shell
  docker exec -it orion /bin/bash
  ```
  
  - Create the predictionEntities and the subscriptions like in the `entities` folder (createPredictionEntities.sh and subscribeReqPrediction.sh).

### Test the solution

1) Validate that the subscription and entities exist (inside the orion container):
```
curl --location --request GET 'http://localhost:1026/ngsi-ld/v1/subscriptions/'
```

```
curl --location --request GET 'http://localhost:1026/ngsi-ld/v1/entities/urn:ngsi-ld:ReqPrediction1'
```

```
curl --location --request GET 'http://localhost:1026/ngsi-ld/v1/entities/urn:ngsi-ld:ResPrediction1'
```

2) Update the `ReqPrediction1`
```

curl --location --request PATCH 'http://localhost:1026/ngsi-ld/v1/entities/urn:ngsi-ld:ReqPrediction1/attrs' \
--header 'Content-Type: application/json' \
--data-raw '{
   "idStation": {
      "type":"Property",
      "value": [VALUE from 1 to 17 in Santander or from 1 to 505 in Barcelona or from 0 to 9 in Malaga] as a String
   },
   "weekday":{
      "type":"Property",
      "value": [VALUE from 1 to 7]
   },
   "hour":{
      "type":"Property",
      "value": [VALUE from 0 to 23]
   },
   "month":{
       "type": "Property",
       "value": [VALUE from 1 to 12]
   },
   "ciudad":{
       "type": "Property",
       "value": ["Santander" or "Barcelona" or "Malaga"]
   },
   "predictionId":{
      "type":"Property",
      "value":"p-1662768034900"
   },
   "socketId":{
      "type":"Property",
      "value":"Fn0kKHEF-dOcr311AAAF"
   }
}'

```
Being:

* idStation: station id. 
(In Malaga each number represents one station name, assigned by their position in the following array: ["Salitre", "Cervantes","El_Palo","Av._de_Andalucia","Camas","Cruz_De_Humilladero","Alcazaba","San_Juan_De_La_Cruz","Pz._de_la_Marina" or "Tejon_y_Rodriguez"])
* month: [1, 2, 3, ..., 12]
* weekday: [1, ..., 7] 1 ->Sunday 7->Saturday
* time: : [0, ... , 23]
* predictionId: String to identify the prediction in the consuming application
* socketId: String to identify the socket with the client in the consuming application


3) See if the `ResPrediction1` changes

```
curl --location --request GET 'http://localhost:1026/ngsi-ld/v1/entities/urn:ngsi-ld:ResPrediction1'
```

Response:
```
{
   "@context":"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
   "id":"urn:ngsi-ld:BarcelonaBikePrediction1",
   "type":"ResPrediction",
   "socketId":{
      "type":"Property",
      "value":"Fn0kKHEF-dOcr311AAAF"
   },
   "predictionId":{
      "type":"Property",
      "value":"p-1662768034900"
   },
   "predictionValue":{
      "type":"Property",
      "value": [PREDICTED VALUE]
   },
   "idStation":{
      "type":"Property",
      "value": [VALUE set at the request]
   },
   "weekday":{
      "type":"Property",
      "value": [VALUE set at the request]
   },
   "hour":{
      "type":"Property",
      "value": [VALUE set at the request]
   },
   "month":{
      "type":"Property",
      "value": [VALUE set at the request]
   }
```


## Production (YODA)

* Run the whole scenario in prod within YODA (only spark). You need to create the predictionEntities and the subscriptions like in the `entities` folder. They are required the creation of entities and the subscription of spark. In the consuming application there are two possibilities:
   - The application receives a notification when the prediction is made
   - The application asks periodically to orion (`urn:ngsi-ld:ResPrediction1`) and see if the prediction was made

```shell
docker compose up -d
```

* Example of petition made to ask for a prediction:

```
curl --location --request PATCH 'http://138.4.22.130/ngsi-ld/v1/entities/urn:ngsi-ld:ReqPrediction1/attrs' \
--header 'Content-Type: application/json' \
--data-raw '{...}'
```

### Testing everything worked

1) Validate that the subscription and entities exist:
```
curl --location --request GET 'http://138.4.22.130/ngsi-ld/v1/subscriptions/'
```

```
curl --location --request GET 'http://138.4.22.130/ngsi-ld/v1/entities/urn:ngsi-ld:ReqPrediction1'
```

```
curl --location --request GET 'http://138.4.22.130/ngsi-ld/v1/entities/urn:ngsi-ld:ResPrediction1'
```

2) Update the `ReqPrediction1` to the direction
```
curl --location --request PATCH 'http://138.4.22.130/ngsi-ld/v1/entities/urn:ngsi-ld:ReqPrediction1/attrs' \
--header 'Content-Type: application/json' \
--data-raw '{...}'
```

3) See if the `ResPrediction1` changes

```
curl --location --request GET 'http://138.4.22.130/ngsi-ld/v1/entities/urn:ngsi-ld:ResPrediction1'
```

Response:
```
{
   ...
```
