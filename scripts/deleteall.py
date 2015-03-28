#!/usr/bin/env python
import requests

URL = "http://nebriak1:8098/types/cdmi/buckets/cdmi/keys"

params = {'keys': 'true'}
r = requests.get(URL, params=params)
data = r.json()

keys_deleted = 0
failures = 0

for key in data['keys']:
    r = requests.delete("%s/%s" % (URL, key))
    if r.status_code in [204]:
        print("deleted key: %s" % key)
        keys_deleted += 1
    else:
        print("delete failed key: %s status: %d" % (key, r.status_code))
        failures += 1

print('deleted %d objects' % keys_deleted)
if failures > 0:
    print("Delete failed %d times" % failures)
