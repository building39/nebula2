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

import hashlib
import hmac

try:
    import requests
except:
    print('bootstrap.py requires the "requests" library. Please install "requests" and run bootstrap again.')
    print('Example: easy_install requests')
    sys.exit(1)

import getopt
import os
from os import listdir
from os.path import isfile, join
import requests
import time

MAX_METADATA_ITEMS = 1024
MAX_METADATA_SIZE = 8192
MAX_METADATA_TOTAL_SIZE = (MAX_METADATA_ITEMS * MAX_METADATA_SIZE)
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
              
ROOT_OWNER_ACL = {
    'acetype': 'ALLOW',
    'identifier': 'OWNER@',
    'aceflags': 'OBJECT_INHERIT, CONTAINER_INHERIT',
    'acemask': 'ALL_PERMS'
}
ROOT_AUTHD_ACL = {
    'acetype': 'ALLOW',
    'identifier': 'AUTHENTICATED@',
    'aceflags': 'OBJECT_INHERIT, CONTAINER_INHERIT',
    'acemask': 'READ'
}

DOMAIN_OWNER_ACL = {
    'acetype': 'ALLOW',
    'identifier': 'OWNER@',
    'aceflags': 'INHERITED, OBJECT_INHERIT, CONTAINER_INHERIT',
    'acemask': 'ALL_PERMS'
}
DOMAIN_AUTHD_ACL = {
    'acetype': 'ALLOW',
    'identifier': 'AUTHENTICATED@',
    'aceflags': 'INHERITED, OBJECT_INHERIT, CONTAINER_INHERIT',
    'acemask': 'READ'
}

# The following is valid for all containers, dataobjects, domains and queues
DATA_SYSTEM_METADATA_CAPABILITIES = {
    "cdmi_assignedsize": "false",
    "cdmi_authentication_methods": ["anonymous", "basic"],
    "cdmi_data_autodelete": "false",
    "cdmi_data_dispersion": "false",
    "cdmi_data_holds": "false",
    "cdmi_data_redundancy": "",
    "cdmi_data_retention": "false",
    "cdmi_encryption": [],
    "cdmi_geographic_placement": "false",
    "cdmi_immediate_redundancy": "",
    "cdmi_infrastructure_redundancy": "",
    "cdmi_latency": "false",
    "cdmi_RPO": "false",
    "cdmi_RTO": "false",
    "cdmi_sanitization_method": [],
    "cdmi_throughput": "false",
    "cdmi_value_hash": ["MD5", "RIPEMD160", "SHA1", "SHA224", "SHA256", "SHA384", "SHA512"]
}

# The following is valid for all containers, dataobjects, domains and queues
STORAGE_SYSTEM_METADATA_CAPABILITIES = {
    "cdmi_acl": "true",
    "cdmi_acount": "false",
    "cdmi_atime": "true",
    "cdmi_ctime": "true",
    "cdmi_mcount": "false",
    "cdmi_mtime": "true",
    "cdmi_size": "true",
}

# The following is valid only for containers.
CONTAINER_CAPABILITIES = {
    "cdmi_copy_container": "false",
    "cdmi_copy_dataobject": "false",
    "cdmi_create_container": "true",
    "cdmi_create_dataobject": "true",
    "cdmi_create_queue": "false",
    "cdmi_create_reference": "false",
    "cdmi_create_value_range": "false",
    "cdmi_delete_container": "true",
    "cdmi_export_container_cifs": "false",
    "cdmi_export_container_nfs": "false",
    "cdmi_export_container_iscsi": "false",
    "cdmi_export_container_occi": "false",
    "cdmi_export_container_webdav": "false",
    "cdmi_deserialize_container": "false",
    "cdmi_deserialize_dataobject": "false",
    "cdmi_deserialize_queue": "false",
    "cdmi_list_children": "true",
    "cdmi_list_children_range": "true",
    "cdmi_modify_deserialize_container": "false",
    "cdmi_modify_metadata": "true",
    "cdmi_move_container": "false",
    "cdmi_move_dataobject": "false",
    "cdmi_post_dataobject": "false",
    "cdmi_post_queue": "false",
    "cdmi_read_metadata": "true",
    "cdmi_read_value": "false",
    "cdmi_read_value_range": "false",
    "cdmi_serialize_container": "false",
    "cdmi_serialize_dataobject": "false",
    "cdmi_serialize_domain": "false",
    "cdmi_serialize_queue": "false",
    "cdmi_snapshot": "false"
}

# The following is valid only for dataobjects.
DATAOBJECT_CAPABILITIES = {
    "cdmi_delete_dataobject": "true",
    "cdmi_modify_deserialize_dataobject": "false",
    "cdmi_modify_metadata": "true",
    "cdmi_modify_value": "true",
    "cdmi_modify_value_range": "true",
    "cdmi_read_metadata": "true",
    "cdmi_read_value": "true",
    "cdmi_read_value_range": "true",
}

# The following is valid only for dataobjects.
# Same as DATAOBJECT_CAPABILITIES, minus object delete capability.
DATAOBJECT_PERMANENT_CAPABILITIES = {
    "cdmi_modify_deserialize_dataobject": "false",
    "cdmi_modify_metadata": "true",
    "cdmi_modify_value": "true",
    "cdmi_modify_value_range": "true",
    "cdmi_read_metadata": "true",
    "cdmi_read_value": "true",
    "cdmi_read_value_range": "true",
}

# The following is valid only for domain objects.
DOMAIN_CAPABILITIES = {
    "cdmi_copy_domain": "false",
    "cdmi_create_domain": "true",
    "cdmi_delete_domain": "true",
    "cdmi_deserialize_domain": "false",
    "cdmi_domain_members": "true",
    "cdmi_domain_summary": "true",
    "cdmi_list_children": "true",
    "cdmi_modify_deserialize_domain": "false",
    "cdmi_modify_metadata": "true",
    "cdmi_move_domain": "false",
    "cdmi_read_metadata": "true",
}

# The following is valid only for queue objects.
# Queues are not currently supported.
QUEUE_CAPABILITIES = {
    "cdmi_copy_queue": "false",
    "cdmi_delete_queue": "false",
    "cdmi_modify_deserialize_queue": "false",
    "cdmi_modify_metadata": "false",
    "cdmi_modify_value": "false",
    "cdmi_move_queue": "false",
    "cdmi_read_metadata": "false",
    "cdmi_read_value": "false",
    "cdmi_reference_queue": "false"
}

# The following is applied to the root container only.
SYSTEM_CAPABILITIES = {
    "cdmi_copy_dataobject_from_queue": "false",
    "cdmi_copy_queue_by_ID": "false",
    "cdmi_create_reference_by_ID": "false",
    "cdmi_create_value_range_by_ID": "false",
    "cdmi_domains": "false",
    "cdmi_dataobjects": "true",
    "cdmi_deserialize_dataobject_by_ID": "false",
    "cdmi_deserialize_queue_by_ID": "false",
    "cdmi_export_cifs": "false",
    "cdmi_export_iscsi": "false",
    "cdmi_export_nfs": "false",
    "cdmi_export_occi_iscsi": "false",
    "cdmi_export_webdav": "false",
    "cdmi_logging": "false",
    "cdmi_metadata_maxitems": MAX_METADATA_ITEMS,
    "cdmi_metadata_maxsize" : MAX_METADATA_SIZE,
    "cdmi_metadata_maxtotalsize": MAX_METADATA_TOTAL_SIZE,
    "cdmi_multipart_mime": "false",
    "cdmi_notification": "false",
    "cdmi_object_access_by_ID": "true",
    "cdmi_object_copy_from_local": "false",
    "cdmi_object_copy_from_remote": "false",
    "cdmi_object_move_from_local": "false",
    "cdmi_object_move_from_remote": "false",
    "cdmi_object_move_from_ID": "false",
    "cdmi_object_move_to_ID": "false",
    "cdmi_post_dataobject_by_ID": "false",
    "cdmi_post_queue_by_ID": "false",
    "cdmi_query": "false",
    "cdmi_query_regex": "false",
    "cdmi_query_contains": "false",
    "cdmi_query_tags": "false",
    "cdmi_query_value": "false",
    "cdmi_queues": "false",
    "cdmi_references": "false",
    "cdmi_security_access_control": "false",
    "cdmi_security_audit": "false",
    "cdmi_security_data_integrity": "true",
    "cdmi_security_immutability": "false",
    "cdmi_security_sanitization": "false",
    "cdmi_serialization_json": "false",
    "cdmi_serialize_container_ID": "false",
    "cdmi_serialize_dataobject_to_ID": "false",
    "cdmi_serialize_domain_to_ID": "false",
    "cdmi_serialize_queue_to_ID": "false",
    "cdmi_snapshots": "false"
}


HEADERS = {"X-CDMI-Specification-Version": "1.1"}
PRINT_SEP = '-------------------------------------------------------------'
METADATA = '{"metadata": { "cdmi_owner": "%(cdmiOwner)s", %(metadata)s}}'
TIME_FORMAT = '%Y-%m-%dT%H:%M:%S:000000Z'
VERSION = '1.0.0'

CDMI_CAPABILITIES_DOMAIN = '/cdmi_capabilities/domain/'
CDMI_CAPABILITIES_CONTAINER = '/cdmi_capabilities/container/'
CDMI_CAPABILITIES_CONTAINER_PERMANENT = '%spermanent/' % CDMI_CAPABILITIES_CONTAINER
CDMI_CAPABILITIES_DATAOBJECT = '/cdmi_capabilities/dataobject/'
CDMI_CAPABILITIES_DATAOBJECT_PERMANENT = '%spermanent/' % CDMI_CAPABILITIES_DATAOBJECT
CDMI_CAPABILITIES_DATAOBJECT_MEMBER = '%s/member/' % CDMI_CAPABILITIES_DATAOBJECT
CDMI_CAPABILITIES_SYSTEM = '/cdmi_capabilities/'

CDMI_SYSTEM_DOMAIN = '/cdmi_domains/system_domain/'

OBJECT_TYPE_CAPABILITY = 'application/cdmi-capability'
OBJECT_TYPE_CONTAINER  = 'application/cdmi-container'
OBJECT_TYPE_DATAOBJECT = 'application/cdmi-object'
OBJECT_TYPE_DOMAIN     = 'application/cdmi-domain'

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
            
            
        # Create the system domain container
        print("...Priming cdmi_domains/system_domain")
        if self.commit:
            self._create_system_domain()
        else:
            print("Dry run: Created system domain.")
            
        # Create the system domain members container
        print("...Priming cdmi_domains/system_domain/cdmi_domain_members")
        if self.commit:
            self._create_system_domain_members()
        else:
            print("Dry run: Created system domain members container.")
            
        # Create the system domain summary container
        print("...Priming cdmi_domains/system_domain/cdmi_domain_summary")
        if self.commit:
            self._create_system_domain_summary()
        else:
            print("Dry run: Created system domain summary container.")

        # Create the system administrator member
        print("...Priming the system administrator")
        if self.commit:
            self._create_system_administrator_member()
        else:
            print("Dry run: Created the system administrator.")

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
            
        # Create the system configuration domain maps object
        print("...Priming system_configuration domain maps")
        if self.commit:
            self._create_system_configuration_domainmap()
        else:
            print("Dry run: Created system configuration domain maps.")
        
        # Create the system default capabilities
        print("...Priming capabilities")
        if self.commit:
            self._create_system_capabilities()
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
            
        # Create the container permanent capabilities
        print("...Priming container permanent capabilities")
        if self.commit:
            self._create_container_permanent_capabilities()
        else:
            print("Dry run: Created container permanent capabilities.")

         # Create 00the dataobject permanent capabilities
        print("...Priming dataobject permanent capabilities")
        if self.commit:
            self._create_dataobject_permanent_capabilities()
        else:
            print("Dry run: Created dataobject permanent capabilities.")
            

        # Create the domain member capabilities
        print("...Priming domain member capabilities")
        if self.commit:
            self._create_domain_member_capabilities()
        else:
            print("Dry run: Created dataobject member capabilities.")

        return
    
    def _create_root(self):
        acls = [ROOT_OWNER_ACL, ROOT_AUTHD_ACL]
        doc = {'capabilitiesURI': CDMI_CAPABILITIES_SYSTEM,
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
        headers['Content-Type'] = OBJECT_TYPE_CONTAINER
        url = 'http://%s:%d/bootstrap/cdmi_domains/' % (self.host, int(self.port))
        self.domains_oid = self._create(headers, url, doc, acls)
        
    def _create_storage_root(self):
        acls = [ROOT_OWNER_ACL, ROOT_AUTHD_ACL, DOMAIN_OWNER_ACL, DOMAIN_AUTHD_ACL]
        doc = {'capabilitiesURI': CDMI_CAPABILITIES_CONTAINER,
               'domainURI': CDMI_SYSTEM_DOMAIN,
               'completionStatus': 'complete',
               'objectName': 'storage_root/',
               'objectType': OBJECT_TYPE_CONTAINER,
               'parentURI': '/',
               'parentID': self.root_oid}
        headers = HEADERS.copy()
        headers['Content-Type'] = OBJECT_TYPE_CONTAINER
        url = 'http://%s:%d/bootstrap/storage_root/' % (self.host, int(self.port))
        self._create(headers, url, doc, acls)
        
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
        headers['Content-Type'] = OBJECT_TYPE_CONTAINER
        url = 'http://%s:%d/bootstrap/cdmi_domains/system_domain' % (self.host, int(self.port))
        self.system_domain_oid = self._create(headers, url, doc, acls)
        
    def _create_system_domain_members(self):
        acls = [ROOT_OWNER_ACL, ROOT_AUTHD_ACL, DOMAIN_OWNER_ACL, DOMAIN_AUTHD_ACL]
        doc = {'capabilitiesURI': CDMI_CAPABILITIES_CONTAINER,
               'domainURI': CDMI_SYSTEM_DOMAIN,
               'completionStatus': 'complete',
               'objectName': 'cdmi_domain_members/',
               'objectType': OBJECT_TYPE_CONTAINER,
               'parentURI': '/cdmi_domains/system_domain/',
               'parentID': self.system_domain_oid}
        headers = HEADERS.copy()
        headers['Content-Type'] = OBJECT_TYPE_CONTAINER
        url = 'http://%s:%d/bootstrap/cdmi_domains/system_domain/cdmi_domain_members/' % (self.host, int(self.port))
        self.system_domain_members_oid = self._create(headers, url, doc, acls)
        
    def _create_system_domain_summary(self):
        acls = [ROOT_OWNER_ACL, ROOT_AUTHD_ACL, DOMAIN_OWNER_ACL, DOMAIN_AUTHD_ACL]
        doc = {'capabilitiesURI': CDMI_CAPABILITIES_CONTAINER,
               'domainURI': CDMI_SYSTEM_DOMAIN,
               'completionStatus': 'complete',
               'objectName': 'cdmi_domain_summary/',
               'objectType': OBJECT_TYPE_CONTAINER,
               'parentURI': '/cdmi_domains/system_domain/',
               'parentID': self.system_domain_oid}
        headers = HEADERS.copy()
        headers['Content-Type'] = OBJECT_TYPE_CONTAINER
        url = 'http://%s:%d/bootstrap/cdmi_domains/system_domain/cdmi_domain_summary/' % (self.host, int(self.port))
        self.system_domain_summary_oid = self._create(headers, url, doc, acls)
        for period in ['yearly/', 'monthly/', 'weekly/', 'daily/']:
            self._create_system_domain_summary_subcontainer(period)
        
    def _create_system_domain_summary_subcontainer(self, period):
        acls = [ROOT_OWNER_ACL, ROOT_AUTHD_ACL, DOMAIN_OWNER_ACL, DOMAIN_AUTHD_ACL]
        doc = {'capabilitiesURI': CDMI_CAPABILITIES_CONTAINER,
               'domainURI': CDMI_SYSTEM_DOMAIN,
               'completionStatus': 'complete',
               'objectName': period,
               'objectType': OBJECT_TYPE_CONTAINER,
               'parentURI': '/cdmi_domains/system_domain/cdmi_domain_summary/',
               'parentID': self.system_domain_summary_oid}
        headers = HEADERS.copy()
        headers['Content-Type'] = OBJECT_TYPE_CONTAINER
        url = 'http://%s:%d/bootstrap/cdmi_domains/system_domain/cdmi_domain_summary/%s' % (self.host, int(self.port), period)
        self._create(headers, url, doc, acls)
        
    def _create_system_administrator_member(self):
        acls = [ROOT_OWNER_ACL, ROOT_AUTHD_ACL, DOMAIN_OWNER_ACL, DOMAIN_AUTHD_ACL]
        credentials = hmac.HMAC(key=self.adminid,msg=self.adminpw, digestmod=hashlib.sha1).hexdigest()
        value = '{"cdmi_member_enabled": "true",' \
                 '"cdmi_member_type": "user", ' \
                 '"cdmi_member_name": "%s",' \
                 '"cdmi_member_credentials": "%s",' \
                 '"cdmi_member_principal": "%s",' \
                 '"cdmi_member_privileges": ["cross_domain", "administrator"],' \
                 '"cdmi_member_groups": []}' % (self.adminid, credentials, self.adminid)

        doc = {'capabilitiesURI': CDMI_CAPABILITIES_DATAOBJECT_MEMBER,
               'domainURI': CDMI_SYSTEM_DOMAIN,
               'completionStatus': 'complete',
               'objectName': self.adminid,
               'objectType': OBJECT_TYPE_DATAOBJECT,
               'parentURI': '/cdmi_domains/system_domain/cdmi_domain_members/',
               'parentID': self.system_domain_members_oid,
               'value': value,
               'valuerange': '0-%d' % (len(value) - 1),
               'valuetransferencoding': "utf-8" }
        headers = HEADERS.copy()
        headers['Content-Type'] = OBJECT_TYPE_CONTAINER
        url = 'http://%s:%d/bootstrap/cdmi_domains/system_domain/cdmi_domain_members/administrator' % (self.host, int(self.port))
        self.system_domain_members_oid = self._create(headers, url, doc, acls)

    def _create_system_configuration(self):
        acls = [ROOT_OWNER_ACL, ROOT_AUTHD_ACL, DOMAIN_OWNER_ACL, DOMAIN_AUTHD_ACL]
        doc = {'capabilitiesURI': CDMI_CAPABILITIES_CONTAINER_PERMANENT,
               'domainURI': CDMI_SYSTEM_DOMAIN,
               'completionStatus': 'complete',
               'objectName': 'system_configuration/',
               'objectType': OBJECT_TYPE_CONTAINER,
               'parentURI': '/',
               'parentID': self.root_oid}
        headers = HEADERS.copy()
        headers['Content-Type'] = OBJECT_TYPE_CONTAINER
        url = 'http://%s:%d/bootstrap/system_configuration/' % (self.host, int(self.port))
        self.system_config_oid = self._create(headers, url, doc, acls)
           
    def _create_system_configuration_environment(self):
        acls = [ROOT_OWNER_ACL, ROOT_AUTHD_ACL, DOMAIN_OWNER_ACL, DOMAIN_AUTHD_ACL]
        doc = {'capabilitiesURI': CDMI_CAPABILITIES_CONTAINER_PERMANENT,
               'domainURI': CDMI_SYSTEM_DOMAIN,
               'completionStatus': 'complete',
               'objectName': 'environment_variables/',
               'objectType': OBJECT_TYPE_CONTAINER,
               'parentURI': '/system_configuration/',
               'parentID': self.system_config_oid}
        headers = HEADERS.copy()
        headers['Content-Type'] = OBJECT_TYPE_CONTAINER
        url = 'http://%s:%d/bootstrap/system_configuration/environment_variables' % (self.host, int(self.port))
        self.system_envvar_oid = self._create(headers, url, doc, acls)
        
    def _create_system_configuration_domainmap(self):
        acls = [ROOT_OWNER_ACL, ROOT_AUTHD_ACL, DOMAIN_OWNER_ACL, DOMAIN_AUTHD_ACL]
        doc = {'capabilitiesURI': CDMI_CAPABILITIES_DATAOBJECT_PERMANENT,
               'domainURI': CDMI_SYSTEM_DOMAIN,
               'completionStatus': 'complete',
               'objectName': 'domain_maps',
               'objectType': OBJECT_TYPE_DATAOBJECT,
               'parentURI': '/system_configuration/',
               'parentID': self.system_config_oid,
               'value': '[]',
               'valuerange': '0-1',
               'valuetransferencoding': 'utf-8'}
        headers = HEADERS.copy()
        headers['Content-Type'] = OBJECT_TYPE_DATAOBJECT
        url = 'http://%s:%d/bootstrap/system_configuration/domain_maps' % (self.host, int(self.port))
        self.system_envvar_oid = self._create(headers, url, doc, acls)
           
    def _create_system_capabilities(self):
        acls = None
        doc = {'capabilities': SYSTEM_CAPABILITIES,
               'objectName': 'cdmi_capabilities/',
               'objectType': OBJECT_TYPE_CAPABILITY,
               'parentURI': '/',
               'parentID': self.root_oid}
        headers = HEADERS.copy()
        headers['Content-Type'] = OBJECT_TYPE_CAPABILITY
        url = 'http://%s:%d/bootstrap/cdmi_capabilities/' % (self.host, int(self.port))
        self.caps_oid = self._create(headers, url, doc, acls)
        
    def _create_container_capabilities(self):
        acls = None
        localdict = CONTAINER_CAPABILITIES.copy()
        localdict.update(STORAGE_SYSTEM_METADATA_CAPABILITIES)
        localdict.update(DATA_SYSTEM_METADATA_CAPABILITIES)
        doc = {'capabilities': localdict,
               'objectName': 'container/',
               'objectType': OBJECT_TYPE_CAPABILITY,
               'parentURI': '/cdmi_capabilities/',
               'parentID': self.caps_oid}
        headers = HEADERS.copy()
        headers['Content-Type'] = OBJECT_TYPE_CAPABILITY
        url = 'http://%s:%d/bootstrap/cdmi_capabilities/container/' % (self.host, int(self.port))
        self.container_caps_oid = self._create(headers, url, doc, acls)
        
    def _create_container_permanent_capabilities(self):
        acls = None
        localdict = CONTAINER_CAPABILITIES.copy()
        localdict.update(STORAGE_SYSTEM_METADATA_CAPABILITIES)
        localdict.update(DATA_SYSTEM_METADATA_CAPABILITIES)
        doc = {'capabilities': localdict,
               'objectName': 'permanent/',
               'objectType': OBJECT_TYPE_CAPABILITY,
               'parentURI': '/cdmi_capabilities/container/',
               'parentID': self.container_caps_oid}
        headers = HEADERS.copy()
        headers['Content-Type'] = OBJECT_TYPE_CAPABILITY
        url = 'http://%s:%d/bootstrap/cdmi_capabilities/container/permanent/' % (self.host, int(self.port))
        self._create(headers, url, doc, acls)
        
    def _create_dataobject_capabilities(self):
        acls = None
        localdict = DATAOBJECT_CAPABILITIES.copy()
        localdict.update(STORAGE_SYSTEM_METADATA_CAPABILITIES)
        localdict.update(DATA_SYSTEM_METADATA_CAPABILITIES)
        doc = {'capabilities': localdict,
               'objectName': 'dataobject/',
               'objectType': OBJECT_TYPE_CAPABILITY,
               'parentURI': '/cdmi_capabilities/',
               'parentID': self.caps_oid}
        headers = HEADERS.copy()
        headers['Content-Type'] = OBJECT_TYPE_CAPABILITY
        url = 'http://%s:%d/bootstrap/cdmi_capabilities/dataobject/' % (self.host, int(self.port))
        self.dataobject_caps_oid = self._create(headers, url, doc, acls)
        
    def _create_dataobject_permanent_capabilities(self):
        acls = None
        localdict = DATAOBJECT_PERMANENT_CAPABILITIES.copy()
        localdict.update(STORAGE_SYSTEM_METADATA_CAPABILITIES)
        localdict.update(DATA_SYSTEM_METADATA_CAPABILITIES)
        doc = {'capabilities': localdict,
               'objectName': 'permanent/',
               'objectType': OBJECT_TYPE_CAPABILITY,
               'parentURI': '/cdmi_capabilities/dataobject/',
               'parentID': self.dataobject_caps_oid}
        headers = HEADERS.copy()
        headers['Content-Type'] = OBJECT_TYPE_CAPABILITY
        url = 'http://%s:%d/bootstrap/cdmi_capabilities/dataobject/permanent/' % (self.host, int(self.port))
        self.dataobject_caps_perm_oid = self._create(headers, url, doc, acls)

    def _create_domain_member_capabilities(self):
        acls = None
        doc = {'capabilities': DATAOBJECT_CAPABILITIES,
               'objectName': 'member/',
               'objectType': OBJECT_TYPE_CAPABILITY,
               'parentURI': '/cdmi_capabilities/dataobject/',
               'parentID': self.dataobject_caps_oid}
        headers = HEADERS.copy()
        headers['Content-Type'] = OBJECT_TYPE_CAPABILITY
        url = 'http://%s:%d/bootstrap/cdmi_capabilities/dataobject/member/' % (self.host, int(self.port))
        self._create(headers, url, doc, acls)

    def _create_domain_capabilities(self):
        acls = None
        localdict = DOMAIN_CAPABILITIES.copy()
        localdict.update(STORAGE_SYSTEM_METADATA_CAPABILITIES)
        localdict.update(DATA_SYSTEM_METADATA_CAPABILITIES)
        doc = {'capabilities': localdict,
               'objectName': 'domain/',
               'objectType': OBJECT_TYPE_CAPABILITY,
               'parentURI': '/cdmi_capabilities/',
               'parentID': self.caps_oid}
        headers = HEADERS.copy()
        headers['Content-Type'] = OBJECT_TYPE_CAPABILITY
        url = 'http://%s:%d/bootstrap/cdmi_capabilities/domain/' % (self.host, int(self.port))
        self.domain_caps_oid = self._create(headers, url, doc, acls)

    def _create(self, headers, url, doc, acls):
#        import sys; sys.path.append('/opt/eclipse/plugins/org.python.pydev_4.3.0.201508182223/pysrc')
#        import pydevd; pydevd.settrace()
        new_headers = headers.copy()
        auth_string = 'Basic %s' % encodestring('%s:%s' % (self.adminid, self.adminpw))
        new_headers["Authorization"] = auth_string
        now = time.gmtime()
        timestamp = time.strftime('%Y-%m-%dT%H:%M:%s.000000Z', now)

        if doc['objectType'] not in [OBJECT_TYPE_CAPABILITY]:
            metadata = {'cdmi_owner': self.adminid}
            if STORAGE_SYSTEM_METADATA_CAPABILITIES['cdmi_acl'] == "true":
                if acls:
                    metadata['cdmi_acls'] = acls
            if STORAGE_SYSTEM_METADATA_CAPABILITIES['cdmi_atime'] == "true":
                metadata['cdmi_atime'] = timestamp
            if STORAGE_SYSTEM_METADATA_CAPABILITIES['cdmi_ctime'] == "true":
                metadata['cdmi_ctime'] = timestamp
            if STORAGE_SYSTEM_METADATA_CAPABILITIES['cdmi_mtime'] == "true":
                metadata['cdmi_mtime'] = timestamp
            if STORAGE_SYSTEM_METADATA_CAPABILITIES['cdmi_acount'] == "true":
                metadata['cdmi_acount'] = 0
            if STORAGE_SYSTEM_METADATA_CAPABILITIES['cdmi_mcount'] == "true":
                metadata['cdmi_mcount'] = 0
            if doc.has_key('value'):
                hash_values = DATA_SYSTEM_METADATA_CAPABILITIES.get('cdmi_value_hash', [])
                if "SHA512" in hash_values:
                    metadata['cdmi_value_hash'] = "SHA512"
                    metadata['cdmi_hash'] = hashlib.sha512(doc['value']).hexdigest()
                elif "SHA384" in hash_values:
                    metadata['cdmi_value_hash'] = "SHA384"
                    metadata['cdmi_hash'] = hashlib.sha384(doc['value']).hexdigest()
                elif "SHA256" in hash_values:
                    metadata['cdmi_value_hash'] = "SHA256"
                    metadata['cdmi_hash'] = hashlib.sha256(doc['value']).hexdigest()
                elif "SHA224" in hash_values:
                    metadata['cdmi_value_hash'] = "SHA224"
                    metadata['cdmi_hash'] = hashlib.sha224(doc['value']).hexdigest()
                elif "SHA1" in hash_values:
                    metadata['cdmi_value_hash'] = "SHA1"
                    metadata['cdmi_hash'] = hashlib.sha1(doc['value']).hexdigest()
                elif "MD5" in hash_values:
                    metadata['cdmi_value_hash'] = "MD5"
                    metadata['cdmi_hash'] = hashlib.md5(doc['value']).hexdigest()
            doc['metadata'] = metadata

        r = requests.put(url=url,
                         data=json.dumps(doc),
                         headers=new_headers,
                         allow_redirects=True)
        if r.status_code in [200, 201, 204]:
            body = json.loads(r.text)
            print('status code: %d' % r.status_code)
            #print('got object: %s' % body)
            oid = body['objectID']
            self.newobjects += 1
            time.sleep(1) # give riak time to index
        elif r.status_code in [409]:
            print("CDMI is already bootstrapped.")
            sys.exit(1)
        else:
           print("Bootstrapping received status code %d - exiting..." % r.status_code)
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
    
