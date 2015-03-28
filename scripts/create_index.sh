curl -v -XPUT "http://nebriak1:8098/search/index/cdmi_idx" -H 'Content-Type: application/json' -d '{"schema": "cdmi"}'

sleep 3

curl -v -XPUT "http://nebriak1:8098/types/cdmi/buckets/cdmi/props" -H "Content-Type: application/json" -d '{"props": {"search_index": "cdmi_idx"}}'
