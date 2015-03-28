#!/usr/bin/env python
import json
import riak
import sys

NODES = [{'host': 'nebriak1', 'pb_port': 8087},
         {'host': 'nebriak2', 'pb_port': 8087},
         {'host': 'nebriak3', 'pb_port': 8087}]

# Connect to riak
client = riak.RiakClient(nodes=NODES)

# set up the bucket
bucket = client.bucket_type('cdmi').bucket('cdmi')

keys_fetched = 0
failures = 0
level = 0
missing_children = []

def printit(name, data):
    if name == '/':
        name = 'root'
    print('*' * 80)
    print('Object: %s' % name)
    print('-' * 80)
    print('%s' % json.dumps(data, indent=2))
    print('*' * 80)
    print('\n' *3)
    
def get_object(name, search_pred, parent=''):
    global keys_fetched
    global missing_children
    global failures
    global level
    resp = client.fulltext_search('cdmi_idx', search_pred)
    if resp['num_found'] == 1:
        try:
            objdata = bucket.get(resp['docs'][0]['objectID']).data
            printit("%s%s" % (parent, objdata['objectName']), objdata)
            keys_fetched += 1
        except e:
            print('Fetch failed.')
            print('name: %s search_pred: %s parent: %s' % (name, search_pred, parent))
            failures += 1
    else:
        if resp['num_found'] == 0:
            missing_children.append(name)
            print('missing child search predicate: %s' % search_pred)
            print('missing child parent:           %s' % parent)
            return
        else:
            print("wrong number of objects! found %d" % resp['num_found'])
            print("object name: %s" % name)
            print("search predicate: %s" % search_pred)
            print('Listed %d objects' % keys_fetched)
            sys.exit(1)

    if 'children' in objdata:
        children = objdata['children']
        prev_parent = parent
        parent = '%s%s' % (parent, objdata['objectName'])
        level += 1
        #print('level %d' % level)
        for child in children:
            get_object(child,
                       'parentURI:\%s AND objectName:%s' % (parent,
                                                           child),
                       parent)
        parent = prev_parent
        level -= 1

get_object('/', 'objectName:\/')

print('Listed %d objects' % keys_fetched)
count = 0
for child in  missing_children:
    print("Missing child: %s" % child)
    count += 1
if count > 0:
    print('%d missing children' % count)
if failures > 0:
    print('%d failures' % failures)
