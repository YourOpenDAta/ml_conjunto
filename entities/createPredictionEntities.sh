curl orion:1026/ngsi-ld/v1/entities -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
{
    "id": "urn:ngsi-ld:ReqBikePrediction1",
    "type": "ReqBikePrediction",
    "predictionId": {
        "value": 0,
        "type": "Property"
      },
      "socketId": {
        "value": 0,
        "type": "Property"
      },
      "idStation":{
        "value": 0,
        "type": "Property"
      },
      "hour":{
        "value": 0,
        "type": "Property"
      },
      "month":{
        "value": 0,
        "type": "Property"
      },
      "weekday": {
        "value": 0,
        "type": "Property"
      },
      "ciudad": {
        "value": 0,
        "type": "Property"
      },
    "@context": [
      "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
    ]
}
EOF



curl orion:1026/ngsi-ld/v1/entities -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
{
  "id": "urn:ngsi-ld:ResBikePrediction1",
  "type": "ResBikePrediction",
  "predictionId": {
    "value": "0",
    "type": "Property"
  },
  "socketId": {
    "value": 0,
    "type": "Property"
  },
  "predictionValue":{
    "value": 0,
    "type": "Property"
  },
  "idStation":{
    "value": 0,
    "type": "Property"
  },
  "weekday":{
    "value": 0,
    "type": "Property"
  },
  "hour": {
    "value": 0,
    "type": "Property"
  },
  "month": {
    "value": 0,
    "type": "Property"
  },
  "@context": [
    "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
  ]
}
EOF