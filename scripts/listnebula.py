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


class GetNebula(object):
    
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
        
    def read(self, object='/'):
        #sys.path.append('/opt/eclipse/plugins/org.python.pydev_4.3.0.201508182223/pysrc')
        #import pydevd; pydevd.settrace()
        # First, read the root.
        url = '%s%s' % (self.url, object)
        headers = self.headers.copy()
#        headers['Content-Type'] = OBJECT_TYPE_CONTAINER
        r = requests.get(url=url,
                         headers=headers,
                         allow_redirects=True)
        if r.status_code in [200, 201, 204]:
            self.objects_found += 1
            body = json.loads(r.text)
            pbody = json.dumps(json.loads(r.text),
                              sort_keys=True,
                              indent=4,
                              separators=(',', ': '))
            print('%s' % pbody)
            children = body.get('children', [])
            for child in children:
                nextobject = '%s%s' % (object, child)
                self.read(object=nextobject)
        else:
           print("listneubula received status code %d - exiting..." % r.status_code)
           print("url is %s" % url)
           print("Found %d objects" % self.objects_found)
           sys.exit(1)


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

    getcdmi = GetNebula(host,  # Nebula host url
                        adminid,
                        adminpw,
                        port,     # Nebula host port
                        verbose)  # print verbose information on progress

    getcdmi.read()
    
    print("Found a total of %d objects" % getcdmi.objects_found)
    
if __name__ == "__main__":
    main(sys.argv[1:])
1111