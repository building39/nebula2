#!/bin/bash

curl -v -uadministrator -H "x-cdmi-specification-version: 1.1" -H "content-type: application/cdmi-container" -H "Accept: application/cdmi-container" -X PUT "http://localhost:8080/cdmi/system_configuration/new_container1/" -d '{"metadata": {"cdmi_domain_enabled": "true"}}'