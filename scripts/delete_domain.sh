#!/bin/bash

curl -v -uadministrator:test -H "x-cdmi-specification-version: 1.1" -X DELETE "http://localhost:8080/cdmi/cdmi_domains/Fuzzcat2/" #|python -mjson.tool
