#!/bin/bash

curl -uadministrator:test -H "x-cdmi-specification-version: 1.1"  -H "Accept: application/cdmi-container" -X GET "http://localhost:8080/cdmi/cdmi_domains/Fuzzcat2/cdmi_domain_summary/monthly/" -v |python -m json.tool
#curl -v -uadministrator -H "x-cdmi-specification-version: 1.1" -H "content-type: application/cdmi-container" -H "Accept: application/cdmi-container" -X GET "http://localhost:8080/cdmi/system_configuration/?children"
