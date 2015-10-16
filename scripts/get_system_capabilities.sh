#!/bin/bash

curl -uadministrator -H "x-cdmi-specification-version: 1.1" -X GET "http://localhost:8080/cdmi/cdmi_capabilities/" |python -m json.tool
