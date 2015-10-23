#!/bin/bash

curl -uadministrator -H "x-cdmi-specification-version: 1.1" -H "content-type: application/cdmi-container" -H "Accept: application/cdmi-container" -X GET "http://localhost:8080/cdmi/cdmi_domains/" -v #|python -m json.tool
