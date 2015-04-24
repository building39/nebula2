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
    from django.template import Context, Template
    from django.conf import settings
    import django
except:
    print('bootstrap.py requires Django. Please install Django and run bootstrap again.')
    print('Example: easy_install django')
    sys.exit(1)
    
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
        tfiles = [ f for f in listdir(self.templatepath) if isfile(join(self.templatepath,f)) ]
        for t in tfiles:
            tf = open('%s/%s' % (self.templatepath, t), 'r')
#            doc = tf.read()
            self.templates[t] = Template(tf.read())
            tf.close()
            
        # Create the root container
        c = Context({"capabilitiesURI": "/cdmi_capabilities/container/permanent/",
                     "completionStatus": "Complete",
                     "domainURI": "/cdmi_domains/system_domain/",
                     "objectName": "rooter/",
                     "objectType": "application/cdmi-container",
                     "children": ROOT_CHILDREN,
                     "childrenrange": "0-%s" % (len(ROOT_CHILDREN) - 1),
                     "snapshots": "",
                     "percentComplete": "",
                     "exports": "",
                     "metadata": ["cdmi_owner\": \"administrator"
                                 ],
                     "objectID": "",
                     "parentID": "",
                     "parentURI": ""
                    })
        rootdoc = json.dumps(json.loads(self.templates["cdmi_containers.dtl"].render(c)))
        headers = HEADERS
        headers['Content-Type'] = 'application/cdmi-container'
        url = 'http://%s:%d/cdmi/' % (self.host, self.port)
        r = requests.put(url=url,
                         data=rootdoc,
                         headers=headers,
                         allow_redirects=True)
        
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
                                    'commit=',
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
        elif opt == '--debug':
            global DEBUG
            DEBUG = True
        elif opt == '--commit':
            commit = arg
        elif opt == '--host':
            host = arg
        elif opt == '--port':
            port = True
        elif opt == '--templates':
            templates = arg
            
    if host is None or templates is None:
        usage()
        sys.exit(1)
    while adminpw = '':
        adminpw = getpass.getpass('Please enter a password for the admin user')
        
    settings.configure(TEMPATE_DEBUG=True,
                       TEMPLATE_DIRS=(templates))
    django.setup()

    bs = Bootstrap(host,  # Nebula host url
                   templates, # pat to templates
                   port,  # Nebula host port
                   adminpw=adminpw,
                   commit=commit,
                   verbose=verbose)  # print verbose information on progress

    bs.bootstrap()
    
    print 'Bootstrapped %d new objects' % bs.newobjects

if __name__ == "__main__":
#    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "bot_server.settings.local")
    from django.core.management import execute_from_command_line
    main(sys.argv[1:])
    
