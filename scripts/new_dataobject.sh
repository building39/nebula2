#!/bin/bash

curl -uadministrator:test -H "x-cdmi-specification-version: 1.1" -H "content-type: application/cdmi-object" -H "Accept: application/cdmi-object" -X PUT "http://localhost:8080/cdmi/new_container7/new_object8.txt" -d '{"valuetransferencoding": "base64", "value": "A new data object is born!"}' -v #| python -mjson.tool
