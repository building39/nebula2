clear;curl -v -include --form "file=@multipart_body;type=application/cdmi-object" --form "file=@new_domain.sh" -X PUT "http://localhost:8080/cdmi/new_container7/multipart5.txt" -H "Content-Type: multipart/mixed" -H "x-cdmi-specification-version: 1.1" -uadministrator:test