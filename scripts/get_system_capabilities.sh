#!/bin/bash

curl -uadministrator:test -H "Accept: application/cdmi-capability" -H "x-cdmi-specification-version: 1.1" -X GET "http://localhost:8080/cdmi/cdmi_capabilities/" -v |python -m json.tool
