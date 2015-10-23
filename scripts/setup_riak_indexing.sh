#!/bin/bash

pushd $NEBULA2_PATH/scripts

RIAK_HOST=nebriak1
RIAK_HTTP_PORT=8098
DELAY=5

# create cdmi bucket type
ssh root@${RIAK_HOST} "riak-admin bucket-type create cdmi '{\"props\":{\"allow_mult\": false, \"last_write_wins\": true, \"big_vclock\": 16, \"small_vclock\": 4}}'"
ssh root@${RIAK_HOST} "riak-admin bucket-type activate cdmi"

sleep ${DELAY}

# register the schema with riak
curl -v -XPUT "http://${RIAK_HOST}:${RIAK_HTTP_PORT}/search/schema/cdmi" -H 'Content-Type:application/xml' --data-binary @../config/cdmi_schema2.xml

sleep ${DELAY}

# create the index
curl -v -XPUT "http://${RIAK_HOST}:${RIAK_HTTP_PORT}/search/index/cdmi_idx" -H 'Content-Type: application/json' -d '{"schema": "cdmi"}'

sleep ${DELAY}

# apply index to bucket type cdmi
curl -v -XPUT "http://${RIAK_HOST}:${RIAK_HTTP_PORT}/types/cdmi/buckets/cdmi/props" -H "Content-Type: application/json" -d '{"props": {"search_index": "cdmi_idx"}}'

popd
