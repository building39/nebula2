#!/bin/bash

curl -uadministrator:test -H "x-cdmi-specification-version: 1.1" -H "Accept: application/cdmi-object" -X DELETE "http://localhost:8080/cdmi/new_container7/new_object8.txt" -v
