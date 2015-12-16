#!/bin/bash

curl -u"administrator:test;realm=system_domain" -H "x-cdmi-specification-version: 1.1"  -H "Accept: application/cdmi-container" -X GET "http://localhost:8080/cdmi/system_configuration/" -v |python -m json.tool
#curl -uadministrator:test -H "x-cdmi-specification-version: 1.1"  -H "Accept: application/cdmi-container" -X GET "http://localhost:8080/cdmi/new_container7/" -v |python -m json.tool
#curl -v -uadministrator -H "x-cdmi-specification-version: 1.1" -H "content-type: application/cdmi-container" -H "Accept: application/cdmi-container" -X GET "http://localhost:8080/cdmi/system_configuration/?children"
