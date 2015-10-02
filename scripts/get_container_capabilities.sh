#!/bin/bash

curl -uadministrator:test -H "x-cdmi-specification-version: 1.1" -H "content-type: application/cdmi-capability" -H "Accept: application/cdmi-capability" -X GET "http://localhost:8080/cdmi/cdmi_capabilities/container/" |python -m json.tool
