#!/usr/bin/env python
'''
A bootstrapping script for Nebula.

This script will bootstrap Nebula with a few necessary objects.

Created on March 31, 2015

@author: mmartin4242@gmail.com
'''
import getpass
import sys

try:
    import requests
except:
    print('bootstrap.py requires the "requests" library. Please install "requests" and run bootstrap again.')
    print('Example: easy_install requests')
    sys.exit(1)

import getopt
import json
import os
from os import listdir
from os.path import isfile, join
import requests
import time

HEADERS = {"X-CDMI-Specification-Version": "1.1"}
PRINT_SEP = '-------------------------------------------------------------'
METADATA = '{"metadata": { "cdmi_administrator": "administrator"}}'
ROOT_CHILDREN = ["cdmi_capabilities/",
                 "cdmi_domains/",
                 "system_configuration/"]
TIME_FORMAT = '%Y-%m-%dT%H:%M:%S:000000Z'
VERSION = '1.0.0'

class Bootstrap(object):

    def __init__(self,
                 host,
                 templatepath,
                 adminpw,
                 port=8080,
                 commit=False,
                 verbose=False):
        self.adminpw = adminpw
        self.commit = commit
        self.newobjects = 0
        self.host = host
        self.port = port
        self.templatepath = templatepath
        self.templates = {}

    def bootstrap(self):
        
        # Create the root container
        print("...Priming root document with %s" % METADATA)
        if self.commit:
            self._create_root()
        else:
            print("Dry run: Created root.")

        # Create the domains container
        print("...Priming domains  with %s" % METADATA)
        if self.commit:
            self._create_domain()
        else:
            print("Dry run: Created root.")
        
        return
    
    def _create_root(self):
        headers = HEADERS
        headers['Content-Type'] = 'application/cdmi-container'
        url = 'http://%s:%d/cdmi/' % (self.host, int(self.port))
        r = requests.put(url=url,
                         data=METADATA,
                         headers=headers,
                         allow_redirects=True)
        if r.status_code in [201]:
            self.newobjects += 1
        elif r.status_code in [409]:
            print("CDMI is already bootstrapped.")
            sys.exit(1)
        else:
           print("Bootstrap received status code %d - exiting..." % r.status_code)
           return
       
    def _create_domain(self):
        headers = HEADERS
        headers['Content-Type'] = 'application/cdmi-container'
        url = 'http://%s:%d/cdmi/cdmi_domains' % (self.host, int(self.port))
        r = requests.put(url=url,
                         data=METADATA,
                         headers=headers,
                         allow_redirects=True)
        if r.status_code in [201]:
            self.newobjects += 1
        elif r.status_code in [409]:
            print("CDMI is already bootstrapped.")
            sys.exit(1)
        else:
           print("Bootstrap received status code %d - exiting..." % r.status_code)
           return

def usage():
    print ('Nebula CDMI Bootstrap')
    print ('Version : %s' % VERSION)
    print ('')
    print ('Usage: '
           '%s --host=[hostname] --port=[port] --templates=[path to template files], --commit=[true|false]'
           % sys.argv[0])
    print ('          [--help]')
    print ('')
    print (' Command Line options:')
    print ('  --adminpw   - Password for the "admin" user.')
    print ('  --help      - Print this enlightening message')
    print ('  --host      - Nebula host url. Required.')
    print ('  --templates - Path to template files. Required.')
    print ('  --port      - Nebula host port. Optional, defaults to 8080.')
    print ('  --commit    - Optional. Bootstrap commited if true, otherwise')
    print ('                the objects that would have been bootstrapped are')
    print ('                displayed.')
    
def main(argv):

#    import sys; sys.path.append('/opt/eclipse/plugins/org.python.pydev_3.9.2.201502050007/pysrc')
#    import pydevd; pydevd.settrace()
    if (len(sys.argv) < 3):
        usage()

    adminpw = ''
    commit = False
    host = None
    port = 8080
    templates = None
    verbose = False

    try:
        opts, _args = getopt.getopt(argv,
                                   '',
                                   ['adminpw=',
                                    'help',
                                    'debug',
                                    'commit',
                                    'host=',
                                    'port=',
                                    'templates=',
                                    'verbose'])
    except getopt.GetoptError, e:
        print ('opt error %s' % e)
        print ('')
        usage()

    for opt, arg in opts:
        if opt in ("--adminpw"):
            adminpw = arg
        elif opt in ("-h", "--help"):
            usage()
        elif opt == '--debug':
            global DEBUG
            DEBUG = True
        elif opt == '--commit':
            commit = True
        elif opt == '--host':
            host = arg
        elif opt == '--port':
            port = True
        elif opt == '--templates':
            templates = arg
            
    if host is None or templates is None:
        usage()
        sys.exit(1)
        
    while adminpw == '':
        adminpw = getpass.getpass('Please enter a password for the admin user')

    bs = Bootstrap(host,  # Nebula host url
                   templates, # pat to templates
                   adminpw,
                   port,  # Nebula host port
                   commit=commit,
                   verbose=verbose)  # print verbose information on progress

    bs.bootstrap()
    
    print 'Bootstrapped %d new objects' % bs.newobjects

if __name__ == "__main__":
    main(sys.argv[1:])
    
