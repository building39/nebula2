#!/usr/bin/env python
from base64 import encodestring
import getopt
import getpass
import json
import requests
import sys

HEADERS = {"X-CDMI-Specification-Version": "1.1"}

OBJECT_TYPE_CAPABILITY = 'application/cdmi-capability'
OBJECT_TYPE_CONTAINER  = 'application/cdmi-container'
OBJECT_TYPE_DATAOBJECT = 'application/cdmi-object'
OBJECT_TYPE_DOMAIN     = 'application/cdmi-domain'

VERSION = '1.0.0'


class CheckNebula(object):
    
    def __init__(self,
                 host,  # Nebula host url
                 adminid,
                 adminpw,
                 port,  # Nebula host port
                 verbose):
        self.host = host
        self.port = int(port)
        self.adminid = adminid
        self.adminpw = adminpw
        self.verbose = verbose
        self.url = 'http://%s:%s/cdmi' % (self.host, self.port)
        auth_string = 'Basic %s' % encodestring('%s:%s' % (self.adminid, self.adminpw))
        self.headers = HEADERS.copy()
        self.headers["Authorization"] = auth_string
        self.objects_found = 0
        self.children_found = 0
        self.parents_found = 0
        self.capabilities_found = 0
        self.capabilities_used = 0
        self.domains_found = 0
        self.domains_used = 0
        self.children_missing = 0
        self.parents_missing = 0
        self.capabilities_missing = 0
        self.domains_missing = 0
        self.child_count_mismatch = 0
        self.containers_found = 0
        self.dataobjects_found = 0
        self.unknown_objects = 0
        
    def check(self, object='/'):
        (status, body) = self.get(object)
        if status in [200, 201, 204]:
            body = json.loads(body)
            print("Found object named '%s'" % object)
            ObjectType = body.get('objectType', None)
            if ObjectType == 'application/cdmi-container':
                self.containers_found += 1
            elif ObjectType == 'application/cdmi-object':
                self.dataobjects_found += 1
            elif ObjectType == 'application/cdmi-domain':
                self.domains_found += 1
            elif ObjectType == 'application/cdmi-capability':
                self.capabilities_found += 1
            else:
                self.unknown_objects += 1
            self.objects_found += 1
            capUri = body.get('capabilitiesURI', None)
            if capUri:
                (status2, body2) = self.get(capUri)
                if status2 in [200, 201, 204]:
                    self.capabilities_used += 1
                else:
                    self.capabilities_missing += 1
                    print("Capability %s missing" % capUri)
            parentUri = body.get('parentURI', None)
            if parentUri:
                (status3, body3) = self.get(parentUri)
                if status3 in [200, 201, 204]:
                    self.parents_found += 1
                else:
                    self.parents_missing += 1
                    print("Object %s is missing parent %s", (body.get('objectName'), parentUri))
            domainUri = body.get('domainURI', None)
            if domainUri:
                (status4, body4) = self.get(domainUri)
                if status4 in [200, 201, 204]:
                    self.domains_used += 1
                else:
                    print("could not find domain %s status: %d" % (domainUri, status4))
                    self.domains_missing += 1
            children = body.get('children', [])
            childrenrange = body.get('childrenrange', None)
            num_children = len(children)
            if childrenrange:
                #import sys; sys.path.append('/opt/eclipse/plugins/org.python.pydev_4.3.0.201508182223/pysrc')
                #import pydevd; pydevd.settrace()
                (start, end) = childrenrange.split('-')
                x = int(end) - int(start) + 1
                if x != num_children:
                    print("Actual number of children: %d" % num_children)
                    print("Calculated number of children: %d" % x)
                    self.child_count_mismatch += 1
            else:
                if num_children:
                    print("No childrenrange found for object with %d children" % num_children)
                    self.child_count_mismatch += 1
            for child in children:
                nextobject = '%s%s' % (object, child)
                print("...next object: %s" % nextobject)
                (status5, body) = self.get(nextobject)
                if status5 in [200, 201, 204]:
                    self.children_found += 1
                    self.check(object=nextobject)
                else:
                    self.children_missing += 1
                    print('...missing child %s' % (child))
        else:
           print("listnebula received status code %d - exiting..." % status)
           print("Found %d objects" % self.objects_found)

    def get(self, object):
        url = '%s%s' % (self.url, object)
        headers = self.headers.copy()
        r = requests.get(url=url,
                         headers=headers,
                         allow_redirects=True)
        return(r.status_code, r.text)

def usage():
    print ('List contents of Nebula server using CDMI')
    print ('Version : %s' % VERSION)
    print ('')
    print ('Usage: '
           '%s --host=[hostname] --port=[port] --adminid --adminpw'
           % sys.argv[0])
    print ('')
    print (' Command Line options:')
    print ('  --adminpw   - Password for the "admin" user. If absent, will be prompted for.')
    print ('  --adminid   - User name for the administrator user. Default: administrator')
    print ('  --help      - Print this enlightening message')
    print ('  --host      - Nebula host url. Required.')
    print ('  --port      - Nebula host port. Optional, defaults to 8080.')

def main(argv):

    if (len(sys.argv) < 3):
        usage()

    adminid = 'administrator'
    adminpw = ''    
    host = None
    port = 8080
    verbose = False

    try:
        opts, _args = getopt.getopt(argv,
                                   '',
                                   ['adminid=',
                                    'adminpw=',
                                    'help',
                                    'debug',
                                   'host=',
                                    'port=',
                                    'verbose'])
    except getopt.GetoptError, e:
        print ('opt error %s' % e)
        print ('')
        usage()

    for opt, arg in opts:
        if opt in ("--adminid"):
            adminid = arg
        elif opt in ("--adminpw"):
            adminpw = arg
        elif opt in ("-h", "--help"):
            usage()
        elif opt == '--debug':
            global DEBUG
            DEBUG = True
        elif opt == '--host':
            host = arg
        elif opt == '--port':
            port = arg
        
    if host is None:
        usage()
        sys.exit(1)
        
    while adminpw == '':
        adminpw = getpass.getpass('Please enter a password for the admin user')

    check = CheckNebula(host,  # Nebula host url
                        adminid,
                        adminpw,
                        port,     # Nebula host port
                        verbose)  # print verbose information on progress

    check.check()
    
    print("Found a total of %d objects" % check.objects_found)
    print("Containers:                       %d" % check.containers_found)
    print("Data Objects:                     %d" % check.dataobjects_found)
    print("Domains:                          %d" % check.domains_found)
    print("Objects with valid domains:       %d" % check.domains_used)
    print("Capabilities:                     %d" % check.capabilities_found)
    print("Objects with valid Capabilities:  %d" % check.capabilities_used)
    print("Children with valid parents:      %d" % check.parents_found)
    print("Children:                         %d" % check.children_found)
    if check.unknown_objects:
        print("Unknown objects:      %d" % check.unknown_objects)
    if check.children_missing:
        print("Children missing:     %d" % check.children_missing)
    if check.capabilities_missing:
        print("Capabilities missing: %d" % check.capabilities_missing)
    if check.domains_missing:
        print("Domains missing:      %d" % check.domains_missing)
    if check.parents_missing:
        print("Parents missing:      %d" % check.parents_missing)
    if check.child_count_mismatch:
        print("Child count errors:   %d" % check.child_count_mismatch)
    
if __name__ == "__main__":
    main(sys.argv[1:])
1111
