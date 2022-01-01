curl \
	-u elastic:passopasso 
	-X DELETE 'http://localhost:9200/shakespeare?ignore_unavailable=true'

curl --header 'Content-type:application/json' \
	-u elastic:passopasso \
	-X PUT 'http://localhost:9200/shakespeare'

curl --header 'Content-type:application/json' \
	-u elastic:passopasso \
	-X PUT 'http://localhost:9200/shakespeare/_mapping' \
	-d \
'{
   "properties": {
    "speaker": {"type": "keyword"},
    "play_name": {"type": "keyword"},
    "line_id": {"type": "integer"},
    "speech_number": {"type": "integer"}
 }
}'

curl -S -H 'Content-Type: application/x-ndjson' \
	-u elastic:passopasso \
	-X POST 'localhost:9200/shakespeare/_bulk?pretty' \
	--data-binary @./es_data/shakespeare_6.0.json
