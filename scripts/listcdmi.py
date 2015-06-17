#!/usr/bin/env python
import json
import riak
import sys
import base64

NODES = [{'host': 'nebriak1', 'pb_port': 8087},
         {'host': 'nebriak2', 'pb_port': 8087},
         {'host': 'nebriak3', 'pb_port': 8087}]

BUCKET_TYPE = 'cdmi'
BUCKET_NAME = 'cdmi'
INDEX = 'cdmi_idx'

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
    print("Entry parent: %s" % parent)
    resp = client.fulltext_search('cdmi_idx', search_pred)
    if resp['num_found'] == 1:
        try:
            objdata = bucket.get(resp['docs'][0]['_yz_rk']).data
            printit("%s%s" % (parent, objdata['objectName']), objdata)
            keys_fetched += 1
        except Exception, e:
            print('Fetch failed. Exception: %s' % e)
            print('name: %s search_pred: %s parent: %s' % (name, search_pred, parent))
            failures += 1
            return
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
        parent = objdata['objectName']
        if 'parentURI' in objdata:
            parentURI = '%s%s' % (objdata['parentURI'], objdata['objectName'])
        else:
            parentURI = '/'
        level += 1
        print('level %d' % level)
        for child in children:
            print("Prev parent: %s parent: %s child: %s" % (prev_parent, parent, child))
            get_object(child,
                       'parentURI:\\%s AND objectName:\\%s' % (parentURI, child)),
        parent = prev_parent
        level -= 1

def main(argv):
    #sys.path.append('/opt/eclipse/plugins/org.python.pydev_4.0.0.201504132356/pysrc')
    #import pydevd; pydevd.settrace()

    get_object('/', 'objectName:\\/')

    print('Listed %d objects' % keys_fetched)
    count = 0
    for child in  missing_children:
        print("Missing child: %s" % child)
        count += 1
    if count > 0:
        print('%d missing children' % count)
    if failures > 0:
        print('%d failures' % failures)

if __name__ == "__main__":
    main(sys.argv[1:])
