curl  -H 'Content-Type: application/x-ndjson' -XPOST 'localhost:9200/tmp/tmptype/_bulk?pretty' --data-binary @/Users/wajih/Downloads/logs/tmp/http.00_00_00-01_00_00.log  > /dev/null
