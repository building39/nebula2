#!/bin/bash

#curl -uadministrator:test -H "x-cdmi-specification-version: 1.1" -H "content-type: application/cdmi-object" -H "Accept: application/cdmi-object" -X PUT "http://localhost:8080/cdmi/new_container7/new_object1.txt" -d '{"valuetransferencoding": "base64", "value": "Z2FyYmFnZQ=="}' -v | python -mjson.tool
curl -u"administrator:test;realm=system_domain" -H "x-cdmi-specification-version: 1.1" -H "content-type: application/cdmi-object" -H "Accept: application/cdmi-object" -X PUT "http://cloud.fuzzcat.net:8080/cdmi/new_container7/new_object1.txt" -d '{"valuetransferencoding": "base64", "value": "Z2FyYmFnZQ=="}' -v | python -mjson.tool
