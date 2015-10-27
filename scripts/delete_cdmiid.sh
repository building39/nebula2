#!/bin/bash

curl -v -uadministrator:test -H "x-cdmi-specification-version: 1.1" -H "content-type: application/cdmi-container" -X DELETE "http://localhost:8080/cdmi/cdmi_objectid/b26a467c560148c3a3e30215632933cf0004524100482244" 
