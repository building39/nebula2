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
import json
import time

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

CDMI_ACE_ACCESS_ALLOW = 0x0
CDMI_ACE_ACCESS_DENY  = 0x1
CDMI_ACE_SYSTEM_AUDIT = 0x2

CDMI_ACE_FLAGS_NONE = 0x00000000
CDMI_ACE_FLAGS_OBJECT_INHERIT_ACE = 0x00000001
CDMI_ACE_FLAGS_CONTAINER_INHERIT_ACE = 0x00000002
CDMI_ACE_FLAGS_NO_PROPAGATE_ACE = 0x00000004
CDMI_ACE_FLAGS_INHERIT_ONLY_ACE = 0x00000008
CDMI_ACE_FLAGS_IDENTIFIER_GROUP = 0x00000040
CDMI_ACE_FLAGS_INHERITED_ACE = 0x00000080

CDMI_ACE_READ_OBJECT = 0x00000001
CDMI_ACE_LIST_CONTAINER = 0x00000001
CDMI_ACE_WRITE_OBJECT = 0x00000002
CDMI_ACE_ADD_OBJECT = 0x00000002
CDMI_ACE_APPEND_DATA = 0x00000004
CDMI_ACE_ADD_SUBCONTAINER = 0x00000004
CDMI_ACE_READ_METADATA = 0x00000008
CDMI_ACE_WRITE_METADATA = 0x00000010
CDMI_ACE_EXECUTE = 0x00000020
CDMI_ACE_TRAVERSE_CONTAINER = 0x00000020
CDMI_ACE_DELETE_OBJECT = 0x00000040
CDMI_ACE_DELETE_SUBCONTAINER = 0x00000040
CDMI_ACE_READ_ATTRIBUTES = 0x00000080
CDMI_ACE_WRITE_ATTRIBUTES = 0x00000100
CDMI_ACE_WRITE_RETENTION = 0x00000200
CDMI_ACE_WRITE_RETENTION_HOLD = 0x00000400
CDMI_ACE_DELETE = 0x00010000
CDMI_ACE_READ_ACL = 0x00020000
CDMI_ACE_WRITE_ACL = 0x00040000
CDMI_ACE_WRITE_OWNER = 0x00080000
CDMI_ACE_SYNCHRONIZE = 0x00100000
CDMI_ACE_ALL_PERMS = CDMI_ACE_READ_OBJECT | CDMI_ACE_LIST_CONTAINER | CDMI_ACE_WRITE_OBJECT | CDMI_ACE_ADD_OBJECT | \
                    CDMI_ACE_APPEND_DATA | CDMI_ACE_ADD_SUBCONTAINER | CDMI_ACE_READ_METADATA | \
                    CDMI_ACE_WRITE_METADATA | CDMI_ACE_EXECUTE | CDMI_ACE_TRAVERSE_CONTAINER | \
                    CDMI_ACE_DELETE_OBJECT | CDMI_ACE_DELETE_SUBCONTAINER | CDMI_ACE_READ_ATTRIBUTES | \
                    CDMI_ACE_WRITE_ATTRIBUTES | CDMI_ACE_WRITE_RETENTION | CDMI_ACE_WRITE_RETENTION_HOLD | \
                    CDMI_ACE_DELETE | CDMI_ACE_READ_ACL | CDMI_ACE_WRITE_ACL | CDMI_ACE_WRITE_OWNER | \
                    CDMI_ACE_SYNCHRONIZE
CDMI_ACE_RW_ALL = CDMI_ACE_READ_OBJECT | CDMI_ACE_LIST_CONTAINER | CDMI_ACE_WRITE_OBJECT | CDMI_ACE_ADD_OBJECT | \
                  CDMI_ACE_APPEND_DATA | CDMI_ACE_ADD_SUBCONTAINER | CDMI_ACE_READ_METADATA | \
                  CDMI_ACE_WRITE_METADATA | CDMI_ACE_EXECUTE | CDMI_ACE_TRAVERSE_CONTAINER | \
                  CDMI_ACE_DELETE_OBJECT | CDMI_ACE_DELETE_SUBCONTAINER | CDMI_ACE_READ_ACL | CDMI_ACE_WRITE_ACL
CDMI_ACE_RW = CDMI_ACE_READ_OBJECT | CDMI_ACE_LIST_CONTAINER | CDMI_ACE_WRITE_OBJECT | CDMI_ACE_ADD_OBJECT | \
              CDMI_ACE_APPEND_DATA | CDMI_ACE_ADD_SUBCONTAINER | CDMI_ACE_READ_METADATA | \
              CDMI_ACE_WRITE_METADATA
              
ROOT_OWNER_ACL = {'acetype': 'ALLOW',
                  'identifier': 'OWNER@',
                  'aceflags': 'OBJECT_INHERIT, CONTAINER_INHERIT',
                  'acemask': 'ALL_PERMS'}
ROOT_AUTHD_ACL = {'acetype': 'ALLOW',
                  'identifier': 'AUTHENTICATED@',
                  'aceflags': 'OBJECT_INHERIT, CONTAINER_INHERIT',
                  'acemask': 'READ'}

DOMAIN_OWNER_ACL = {'acetype': 'ALLOW',
                    'identifier': 'OWNER@',
                    'aceflags': 'INHERITED, OBJECT_INHERIT, CONTAINER_INHERIT',
                    'acemask': 'ALL_PERMS'}
DOMAIN_AUTHD_ACL = {'acetype': 'ALLOW',
                    'identifier': 'AUTHENTICATED@',
                    'aceflags': 'INHERITED, OBJECT_INHERIT, CONTAINER_INHERIT',
                    'acemask': 'READ'}
            
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
METADATA = '{"metadata": { "cdmi_owner": "%(cdmiOwner)s", %(metadata)s}}'
TIME_FORMAT = '%Y-%m-%dT%H:%M:%S:000000Z'
VERSION = '1.0.0'

CDMI_CAPABILITIES_DOMAIN = '/cdmi_capabilities/domain'
CDMI_SYSTEM_DOMAIN = '/cdmi_domains/system_domain/'

OBJECT_TYPE_CONTAINER = 'application/cdmi-container'
OBJECT_TYPE_DOMAIN    = 'application/cdmi-domain'

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
        return
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
        acls = [ROOT_OWNER_ACL, ROOT_AUTHD_ACL]
        doc = {'capabilitiesURI': CDMI_CAPABILITIES_DOMAIN,
               'domainURI': CDMI_SYSTEM_DOMAIN,
               'completionStatus': 'complete',
               'objectName': '/',
               'objectType': OBJECT_TYPE_CONTAINER}
        headers = HEADERS.copy()
        headers['Content-Type'] = OBJECT_TYPE_CONTAINER
        url = 'http://%s:%d/bootstrap/' % (self.host, int(self.port))
        self.root_oid = self._create(headers, url, doc, acls)
       
    def _create_domain(self):
        acls = [DOMAIN_OWNER_ACL, DOMAIN_AUTHD_ACL]
        doc = {'capabilitiesURI': CDMI_CAPABILITIES_DOMAIN,
               'domainURI': CDMI_SYSTEM_DOMAIN,
               'completionStatus': 'complete',
               'objectName': 'cdmi_domains/',
               'objectType': OBJECT_TYPE_CONTAINER,
               'parentURI': '/',
               'parentID': self.root_oid}
        headers = HEADERS.copy()
        headers['Content-Type'] = 'application/cdmi-container'
        url = 'http://%s:%d/bootstrap/cdmi_domains/' % (self.host, int(self.port))
        self.domains_oid = self._create(headers, url, doc, acls)
        
    def _create_system_domain(self):
        acls = [ROOT_OWNER_ACL, ROOT_AUTHD_ACL, DOMAIN_OWNER_ACL, DOMAIN_AUTHD_ACL]
        doc = {'capabilitiesURI': CDMI_CAPABILITIES_DOMAIN,
               'domainURI': CDMI_SYSTEM_DOMAIN,
               'completionStatus': 'complete',
               'objectName': 'system_domain/',
               'objectType': OBJECT_TYPE_DOMAIN,
               'parentURI': '/cdmi_domains/',
               'parentID': self.domains_oid}
        headers = HEADERS.copy()
        headers['Content-Type'] = 'application/cdmi-container'
        url = 'http://%s:%d/bootstrap/cdmi_domains/system_domain' % (self.host, int(self.port))
        self.system_domain_oid = self._create(headers, url, doc, acls)
           
    def _create_system_configuration(self):
        headers = HEADERS.copy()
        headers['Content-Type'] = 'application/cdmi-container'
        url = 'http://%s:%d/bootstrap/system_configuration/' % (self.host, int(self.port))
        self._create(headers, url, METADATA % (self.adminid, SYS_DOMAIN_ACL))
           
    def _create_system_configuration_environment(self):
        headers = HEADERS.copy()
        headers['Content-Type'] = 'application/cdmi-container'
        url = 'http://%s:%d/bootstrap/system_configuration/environment_variables' % (self.host, int(self.port))
        self._create(headers, url, METADATA % (self.adminid, SYS_DOMAIN_ACL))
           
    def _create_capabilities(self):
        headers = HEADERS.copy()
        headers['Content-Type'] = 'application/cdmi-capability'
        url = 'http://%s:%d/bootstrap/cdmi_capabilities/' % (self.host, int(self.port))
        self._create(headers, url, DEFAULT_CAPABILITIES)
        
    def _create_container_capabilities(self):
        headers = HEADERS.copy()
        headers['Content-Type'] = 'application/cdmi-capability'
        url = 'http://%s:%d/bootstrap/cdmi_capabilities/container/' % (self.host, int(self.port))
        self._create(headers, url, CONTAINER_CAPABILITIES)
        
    def _create_dataobject_capabilities(self):
        headers = HEADERS.copy()
        headers['Content-Type'] = 'application/cdmi-capability'
        url = 'http://%s:%d/bootstrap/cdmi_capabilities/dataobject/' % (self.host, int(self.port))
        self._create(headers, url, DATAOBJECT_CAPABILITIES)
        
    def _create_domain_capabilities(self):
        headers = HEADERS.copy()
        headers['Content-Type'] = 'application/cdmi-capability'
        url = 'http://%s:%d/bootstrap/cdmi_capabilities/domain' % (self.host, int(self.port))
        self._create(headers, url, DOMAIN_CAPABILITIES)
           
    def _create(self, headers, url, doc, acls):
        new_headers = headers.copy()
        auth_string = 'Basic %s' % encodestring('%s:%s' % (self.adminid, self.adminpw))
        new_headers["Authorization"] = auth_string
        now = time.gmtime()
        timestamp = time.strftime('%Y-%m-%dT%H:%M:%s.000000Z', now)

        metadata = {'cdmi_owner': self.adminid,
                    'cdmi_acls': acls,
                    'cdmi_atime': timestamp,
                    'cdmi_ctime': timestamp,
                    'cdmi_mtime': timestamp}
        doc['metadata'] = metadata

        r = requests.put(url=url,
                         data=json.dumps(doc),
                         headers=new_headers,
                         allow_redirects=True)
        if r.status_code in [200, 201, 204]:
            body = json.loads(r.text)
            print('status code: %d' % r.status_code)
            print('got object: %s' % body)
            oid = body['objectID']
            self.newobjects += 1
            time.sleep(1) # give riak time to index
        elif r.status_code in [409]:
            print("CDMI is already bootstrapped.")
            sys.exit(1)
        else:
           print("Bootstrapping received status code %d - exiting..." % r.status_code)
           sys.exit(1)
           sys.exit(1)
        return oid

def usage():
    print ('Nebula CDMI Bootstrap')
    print ('Version : %s' % VERSION)
    print ('')
    print ('Usage: '
           '%s --host=[hostname] --port=[port] --commit'
           % sys.argv[0])
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
    
