#!/bin/bash

curl -v -uadministrator -H "Accept: application/cdmi-container" -H "x-cdmi-specification-version: 1.1" -H "content-type: application/cdmi-container" -X PUT "http://localhost:8080/cdmi/system_configuration/" -d '{"metadata": {"My_Metadata": "garbage"}}'
