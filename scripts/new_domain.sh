#!/bin/bash

curl -uadministrator:test -H "x-cdmi-specification-version: 1.1" -H "content-type: application/cdmi-domain" -H "Accept: application/cdmi-domain" -X PUT "http://localhost:8080/cdmi/cdmi_domains/Fuzzcat2/" -d '{"metadata": {"cdmi_domain_enabled": "true"}}' # | python -mjson.tool
