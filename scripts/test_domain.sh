#!/bin/bash

curl -v -uadministrator -H "x-cdmi-specification-version: 1.1" -H "content-type: application/cdmi-dataobject" -X PUT "http://localhost:8080/cdmi/system_configuration/domain_maps" -d '{"value": [{"(cloud)[.]fuzzcat[.]net$": "fuzzcat"}]}'
