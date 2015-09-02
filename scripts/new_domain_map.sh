#!/bin/bash

curl -v -uadministrator -H "x-cdmi-specification-version: 1.1" -H "content-type: application/cdmi-object" -H "Accept: application/cdmi-object" -X PUT "http://localhost:8080/cdmi/system_configuration/domain_maps" -d '{"value": [{"(cloud)[.]fuzzcat[.]net$": "Fuzzcat/"}]}'
