#!/bin/bash
curl -v -XPUT "http://nebriak1:8098/search/schema/cdmi" -H 'Content-Type:application/xml' --data-binary @../config/cdmi_schema.xml
