#!/usr/bin/env python
'''
A bootstrapping script for Nebula.

This script will bootstrap Nebula with a few necessary objects.

Created on March 31, 2015

@author: mmartin4242@gmail.com
'''
import getpass
import sys
from base64 import encodestring

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

DOMAIN_ACL = '"cdmi_acl": [{"aceflags": "0x80", ' \
                                  '"acemask": "0x1f07fff", ' \
                                  '"acetype": "0x0", ' \
                                  '"identifier": "ADMINISTRATOR@"}, ' \
                                 '{"aceflags": "0x83", ' \
                                  '"acemask": "0x1f07fff", ' \
                                  '"acetype": "0x0", ' \
                                  '"identifier": "ADMINISTRATOR@"}] '
SYS_DOMAIN_ACL = '"cdmi_acl": [ {"aceflags": "0x0", ' \
                                  '"acemask": "0x1f07fff", ' \
                                  '"acetype": "0x0", ' \
                                  '"identifier": "ADMINISTRATOR@"}, ' \
                                 '{"aceflags": "0xb", ' \
                                  '"acemask": "0x1f07fff", ' \
                                  '"acetype": "0x0", ' \
                                  '"identifier": "ADMINISTRATOR@"}, ' \
                                '{"aceflags": "0x80", ' \
                                  '"acemask": "0x1f07fff", ' \
                                  '"acetype": "0x0", ' \
                                  '"identifier": "ADMINISTRATOR@"}, ' \
                                 '{"aceflags": "0x83", ' \
                                  '"acemask": "0x1f07fff", ' \
                                  '"acetype": "0x0", ' \
                                  '"identifier": "ADMINISTRATOR@"}] '
ROOT_ACL = '"cdmi_acl": [{"aceflags": "0x0", ' \
                                  '"acemask": "0x1f07fff", ' \
                                  '"acetype": "0x0", ' \
                                  '"identifier": "ADMINISTRATOR@"}, ' \
                                 '{"aceflags": "0xb", ' \
                                  '"acemask": "0x1f07fff", ' \
                                  '"acetype": "0x0", ' \
                                  '"identifier": "ADMINISTRATOR@"}] '
DEFAULT_CAPABILITIES = '{"cdmi_domains": "true", ' \
                        '"cdmi_dataobjects": "true", ' \
                        '"cdmi_object_access_by_ID":"true", ' \
                        '"cdmi_object_copy_from_local": "true", ' \
                        '"cdmi_object_move_from_ID": "true", ' \
                        '"cdmi_object_move_from_local": "true"}'
CONTAINER_CAPABILITIES = '{"cdmi_acl": "true", ' \
                         '"cdmi_atime": "true", ' \
                         '"cdmi_copy_container": "true", ' \
                         '"cdmi_copy_dataobject": "true", ' \
                         '"cdmi_create_container": "true", ' \
                         '"cdmi_create_databoject": "true", ' \
                         '"cdmi_ctime": "true", ' \
                         '"cdmi_delete_container": "true", ' \
                         '"cdmi_delete_dataobject": "true", ' \
                         '"cdmi_list_children": "true", ' \
                         '"cdmi_list_children_range": "true", ' \
                         '"cdmi_modify_metadata": "true", ' \
                         '"cdmi_move_container": "true", ' \
                         '"cdmi_move_dataobject": "true", ' \
                         '"cdmi_mtime": "true", ' \
                         '"cdmi_read_metadata": "true", ' \
                         '"cdmi_size": "true", ' \
                         '"cdmi_versioning": "all"}'
DATAOBJECT_CAPABILITIES = '{"cdmi_acl": "true", ' \
                           '"cdmi_atime": "true", ' \
                           '"cdmi_ctime": "true", ' \
                           '"cdmi_delete_dataobject": "true", ' \
                           '"cdmi_modify_metadata": "true", ' \
                           '"cdmi_modify_value": "true", ' \
                           '"cdmi_modify_value_range": "true", ' \
                           '"cdmi_read_metadata": "true", ' \
                           '"cdmi_read_value": "true", ' \
                           '"cdmi_read_value_range": "true", ' \
                           '"cdmi_size": "true", ' \
                           '"cdmi_versioning": "all"}'
DOMAIN_CAPABILITIES = '{"cdmi_acl": "true", ' \
                       '"cdmi_atime": "true", ' \
                       '"cdmi_copy_domain": "false", ' \
                       '"cdmi_create_container": "true", ' \
                       '"cdmi_create_domain": "true", ' \
                       '"cdmi_ctime": "true", ' \
                       '"cdmi_delete_container": "true", ' \
                       '"cdmi_delete_domain": "true", ' \
                       '"cdmi_domain_members": "true", ' \
                       '"cdmi_domain_summary": "true", ' \
                       '"cdmi_list_children": "true", ' \
                       '"cdmi_modify_metadata": "true", ' \
                       '"cdmi_mtime": "true", ' \
                       '"cdmi_read_metadata": "true", ' \
                       '"cdmi_size": "true", ' \
                       '"cdmi_versioning": "all"}'

HEADERS = {"X-CDMI-Specification-Version": "1.1"}
PRINT_SEP = '-------------------------------------------------------------'
METADATA = '{"metadata": { "cdmi_owner": "%s", %s}}'
TIME_FORMAT = '%Y-%m-%dT%H:%M:%S:000000Z'
VERSION = '1.0.0'

class Bootstrap(object):

    def __init__(self,
                 host,
                 adminid,
                 adminpw,
                 port=8080,
                 commit=False,
                 verbose=False):
        self.adminid = adminid
        self.adminpw = adminpw
        self.commit = commit
        self.newobjects = 0
        self.host = host
        self.port = port

    def bootstrap(self):
        
        # Create the root container
        print("...Priming root document")
        if self.commit:
            self._create_root()
        else:
            print("Dry run: Created root.")

        # Create the domains container
        print("...Priming cdmi_domains")
        if self.commit:
            self._create_domain()
        else:
            print("Dry run: Created cdmi_domains.")
            
        # Create the domains container
        print("...Priming cdmi_domains/system_domain")
        if self.commit:
            self._create_system_domain()
        else:
            print("Dry run: Created cdmi_domains.")
            
        # Create the system configuration container
        print("...Priming system_configuration")
        if self.commit:
            self._create_system_configuration()
        else:
            print("Dry run: Created system configuration.")
    
    # Create the system configuration environment variables container
        print("...Priming system_configuration environment variables")
        if self.commit:
            self._create_system_configuration_environment()
        else:
            print("Dry run: Created system configuration environment variables.")
        
        # Create the system default capabilities
        print("...Priming capabilities")
        if self.commit:
            self._create_capabilities()
        else:
            print("Dry run: Created capabilities.")
            
        # Create the container container capabilities
        print("...Priming container capabilities")
        if self.commit:
            self._create_container_capabilities()
        else:
            print("Dry run: Created container capabilities.")
            
        # Create the dataobject default capabilities
        print("...Priming dataobject capabilities")
        if self.commit:
            self._create_dataobject_capabilities()
        else:
            print("Dry run: Created dataobject capabilities.")
            
        # Create the domain default capabilities
        print("...Priming domain capabilities")
        if self.commit:
            self._create_domain_capabilities()
        else:
            print("Dry run: Created domain capabilities.")
            
        return
    
    def _create_root(self):
        headers = HEADERS
        headers['Content-Type'] = 'application/cdmi-container'
        url = 'http://%s:%d/cdmi/' % (self.host, int(self.port))
        self._create(headers, url, METADATA % (self.adminid, ROOT_ACL))
       
    def _create_domain(self):
        headers = HEADERS
        headers['Content-Type'] = 'application/cdmi-container'
        url = 'http://%s:%d/cdmi/cdmi_domains/' % (self.host, int(self.port))
        self._create(headers, url, METADATA % (self.adminid, DOMAIN_ACL))
        
    def _create_system_domain(self):
        headers = HEADERS
        headers['Content-Type'] = 'application/cdmi-container'
        url = 'http://%s:%d/cdmi/cdmi_domains/system_domain' % (self.host, int(self.port))
        self._create(headers, url, METADATA % (self.adminid, SYS_DOMAIN_ACL))
           
    def _create_system_configuration(self):
        headers = HEADERS
        headers['Content-Type'] = 'application/cdmi-container'
        url = 'http://%s:%d/cdmi/system_configuration/' % (self.host, int(self.port))
        self._create(headers, url, METADATA % (self.adminid, SYS_DOMAIN_ACL))
           
    def _create_system_configuration_environment(self):
        headers = HEADERS
        headers['Content-Type'] = 'application/cdmi-container'
        url = 'http://%s:%d/cdmi/system_configuration/environment_variables' % (self.host, int(self.port))
        self._create(headers, url, METADATA % (self.adminid, SYS_DOMAIN_ACL))
           
    def _create_capabilities(self):
        headers = HEADERS
        headers['Content-Type'] = 'application/cdmi-capability'
        url = 'http://%s:%d/cdmi/cdmi_capabilities/' % (self.host, int(self.port))
        self._create(headers, url, DEFAULT_CAPABILITIES)
        
    def _create_container_capabilities(self):
        headers = HEADERS
        headers['Content-Type'] = 'application/cdmi-capability'
        url = 'http://%s:%d/cdmi/cdmi_capabilities/container/' % (self.host, int(self.port))
        self._create(headers, url, CONTAINER_CAPABILITIES)
        
    def _create_dataobject_capabilities(self):
        headers = HEADERS
        headers['Content-Type'] = 'application/cdmi-capability'
        url = 'http://%s:%d/cdmi/cdmi_capabilities/dataobject/' % (self.host, int(self.port))
        self._create(headers, url, DATAOBJECT_CAPABILITIES)
        
    def _create_domain_capabilities(self):
        headers = HEADERS
        headers['Content-Type'] = 'application/cdmi-capability'
        url = 'http://%s:%d/cdmi/cdmi_capabilities/domain' % (self.host, int(self.port))
        self._create(headers, url, DOMAIN_CAPABILITIES)
           
    def _create(self, headers, url, data):
        new_headers = headers.copy()
        auth_string = 'Basic %s' % encodestring('%s:%s' % (self.adminid, self.adminpw))
        new_headers["Authorization"] = auth_string
        r = requests.put(url=url,
                         data=data,
                         headers=new_headers,
                         allow_redirects=True)
        if r.status_code in [201]:
            self.newobjects += 1
            time.sleep(1) # give riak time to index
        elif r.status_code in [409]:
            print("CDMI is already bootstrapped.")
            sys.exit(1)
        else:
           print("Bootstrapping received status code %d - exiting..." % r.status_code)
           sys.exit(1)

def usage():
    print ('Nebula CDMI Bootstrap')
    print ('Version : %s' % VERSION)
    print ('')
    print ('Usage: '
           '%s --host=[hostname] --port=[port] --commit'
           % sys.argv[0])
    print ('          [--help]')
    print ('')
    print (' Command Line options:')
    print ('  --adminpw   - Password for the "admin" user. If absent, will be prompted for.')
    print ('  --adminid   - User name for the administrator user. Default: administrator')
    print ('  --help      - Print this enlightening message')
    print ('  --host      - Nebula host url. Required.')
    print ('  --port      - Nebula host port. Optional, defaults to 8080.')
    print ('  --commit    - Optional. Bootstrap committed if present, otherwise')
    print ('                the objects that would have been bootstrapped are')
    print ('                displayed.')
    
def main(argv):

#    import sys; sys.path.append('/opt/eclipse/plugins/org.python.pydev_3.9.2.201502050007/pysrc')
#    import pydevd; pydevd.settrace()
    if (len(sys.argv) < 3):
        usage()

    adminid = 'administrator'
    adminpw = ''
    commit = False
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
                                    'commit',
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
        elif opt == '--commit':
            commit = True
        elif opt == '--host':
            host = arg
        elif opt == '--port':
            port = arg
            
    if host is None:
        usage()
        sys.exit(1)
        
    while adminpw == '':
        adminpw = getpass.getpass('Please enter a password for the admin user')

    bs = Bootstrap(host,  # Nebula host url
                   adminid,
                   adminpw,
                   port,  # Nebula host port
                   commit=commit,
                   verbose=verbose)  # print verbose information on progress

    bs.bootstrap()
    
    print 'Bootstrapped %d new objects' % bs.newobjects

if __name__ == "__main__":
    main(sys.argv[1:])
    
