#!/bin/bash

curl -v -uadministrator:test -H "x-cdmi-specification-version: 1.1" -H "content-type: application/cdmi-capability" -H "Accept: application/cdmi-container" -X PUT "http://localhost:8080/cdmi/cdmi_capabilities/container/" -d '{"capabilities": {"cdmi_acount": "true"}}'
curl -v -uadministrator:test -H "x-cdmi-specification-version: 1.1" -H "content-type: application/cdmi-capability" -H "Accept: application/cdmi-container" -X PUT "http://localhost:8080/cdmi/cdmi_capabilities/container/" -d '{"capabilities": {"cdmi_mcount": "true"}}'
