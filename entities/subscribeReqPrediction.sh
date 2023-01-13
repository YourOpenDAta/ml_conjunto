curl -v  localhost:1026/ngsi-ld/v1/subscriptions/ -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
{
  "description": "A subscription to get request predictions for bikes in Santander and Barcelona",
  "type": "Subscription",
  "entities": [{
    "id": "urn:ngsi-ld:ReqBikePrediction1",
    "type": "ReqBikePrediction"
    }],
  "watchedAttributes": [
      "predictionId",
      "socketId",
      "idStation",
      "month",
      "hour",
      "weekday",
      "ciudad"
    ],
  "notification": {
    "endpoint": {
      "uri": "http://spark-master-bike-conjunto:9002",
      "accept": "application/json"
    }
  },
    "@context": [
        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
    ]
}
EOF
