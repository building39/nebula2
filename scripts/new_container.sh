#!/bin/bash

curl -v -uadministrator:test -H "x-cdmi-specification-version: 1.1" -H "content-type: application/cdmi-container" -H "Accept: application/cdmi-container" -X PUT "http://localhost:8080/cdmi/new_container7/" -d '{"metadata": {"cdmi_domain_enabled": "true"}}' | python -mjson.tool
#curl -v -u"administrator:test;realm=system_domain,another=option" -H "x-cdmi-specification-version: 1.1" -H "content-type: application/cdmi-container" -H "Accept: application/cdmi-container" -X PUT "http://cloud.fuzzcat.net:8080/cdmi/new_container7/" -d '{"metadata": {"cdmi_domain_enabled": "false"}}' | python -mjson.tool
#curl -v -uadministrator:test -H "x-cdmi-specification-version: 1.1" -H "content-type: application/cdmi-container" -H "Accept: application/cdmi-container" -X PUT "http://localhost:8080/cdmi/new_container1/child1/" -d '{"metadata": {"cdmi_domain_enabled": "true"}}'
#curl -v -uadministrator:test -H "x-cdmi-specification-version: 1.1" -H "content-type: application/cdmi-container" -H "Accept: application/cdmi-container" -X PUT "http://localhost:8080/cdmi/new_container1/child2/" -d '{"metadata": {"cdmi_domain_enabled": "true"}}'
#curl -v -uadministrator:test -H "x-cdmi-specification-version: 1.1" -H "content-type: application/cdmi-container" -H "Accept: application/cdmi-container" -X PUT "http://localhost:8080/cdmi/new_container1/child3/" -d '{"metadata": {"cdmi_domain_enabled": "true"}}'
#curl -v -uadministrator:test -H "x-cdmi-specification-version: 1.1" -H "content-type: application/cdmi-container" -H "Accept: application/cdmi-container" -X PUT "http://localhost:8080/cdmi/new_container1/child2/grandchild1/" -d '{"metadata": {"cdmi_domain_enabled": "true"}}'
