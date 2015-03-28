#!/usr/bin/env python
import json
import requests

URL = "http://nebriak1:8098/types/cdmi/buckets/cdmi/keys"

params = {'keys': 'true'}
r = requests.get(URL, params=params)

try:
    data = r.json()
except e:
    print r.text()
    exit(0)

keys_fetched = 0
failures = 0

for key in data['keys']:
    r = requests.get("%s/%s" % (URL, key))
    if r.status_code in [200]:
        print("Key: %s" % key)
        try:
            data = json.dumps(r.json(), indent=4)
        except:
            data = r.text
        print("Data: %s" % data)
        keys_fetched += 1
    else:
        print("Get failed key: %s status: %d" % (key, r.status_code))
        failures += 1

print('Listed %d objects' % keys_fetched)
if failures > 0:
    print("GET failed %d times" % failures)
