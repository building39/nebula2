%% @author mmartin
%% @doc Include file for nebula.

%% ====================================================================
%% types
%% @todo tighten down these types.
%% ====================================================================
-import(cowboy_req, [req/0]).
-type path()             :: string().

-type content_type()     :: string().
-type yz_index()         :: string().
-type info()             :: term().
-type json_value()       :: list().
-type headers()          :: list().
-type method()           :: string().
-type object_name()      :: nonempty_string().
-type object_oid()       :: nonempty_string().
-type object_type()      :: content_type().
-type object_uri()       :: path()|nil().
-type parent_uri()       :: path().
-type response()         :: {term()|integer(), headers(), string()}.
-type search_predicate() :: nonempty_string().
-type cdmi_state()       :: {pid(), map()}.

%% Debug macros
-define(LOG_ENTRY, lager:debug("Entry")).
-define(LOG_EXIT, lager:debug("Exit")).

%% Miscellaneous macros
-define(DEFAULT_ADMINISTRATOR, "administrator").
-define(DEFAULT_LOCATION, "US-TX").
-define(CDMI_VERSION, "1.1").
-define(FUZZCAT_SNMP_NUMBER, "045241").
-define(HEX_REGEXP, "[0-9a-f]*").
-define(OID_LENGTH, 48).
-define(OID_SUFFIX, "00" ++ ?FUZZCAT_SNMP_NUMBER ++ "0048").
-define(UUID_LENGTH, 32).

%% Memcached parameters
-define(MEMCACHE, nebula_memcache).
-define(MEMCACHE_EXPIRY, 600).   %% Expire after 60 seconds.

%% Header macros.
-define(ACCEPT_HEADER, "accept").
-define(CONTENT_TYPE_HEADER, "content-type").
-define(VERSION_HEADER, "x-cdmi-specification-version").

%% Content types
-define(CONTENT_TYPE_CDMI_CAPABILITY,      "application/cdmi-capability").
-define(CONTENT_TYPE_CDMI_CONTAINER,       "application/cdmi-container").
-define(CONTENT_TYPE_CDMI_DATAOBJECT,      "application/cdmi-dataobject").
-define(CONTENT_TYPE_CDMI_DOMAIN,          "application/cdmi-domain").
-define(CONTENT_TYPE_CDMI_CAPABILITY_JSON, "application/cdmi-capability+json").
-define(CONTENT_TYPE_CDMI_CONTAINER_JSON,  "application/cdmi-container+json").
-define(CONTENT_TYPE_CDMI_DATAOBJECT_JSON, "application/cdmi-dataobject+json").
-define(CONTENT_TYPE_CDMI_DOMAIN_JSON,     "application/cdmi-domain+json").

%% Capability URIs
-define(CONTAINER_CAPABILITY_URI, "/cdmi_capabilities/container").
-define(DATAOBJECT_CAPABILITY_URI, "/cdmi_capabilities/dataobject").
-define(DOMAIN_CAPABILITY_URI, "/cdmi_capabilities/domain").
-define(DOMAIN_SUMMARY_CAPABILITY_URI, "/cdmi_capabilities/domain/summary").
-define(PERMANENT_CONTAINER_CAPABILITY_URI, "/cdmi_capabilities/container/permanent").

%% System domain URI
-define(SYSTEM_DOMAIN_URI, "/cdmi_domains/system_domain").

%% Domain Maps Query
-define(DOMAIN_MAPS_QUERY, "domainURI:\\" ++ ?SYSTEM_DOMAIN_URI ++
            "/ AND parentURI:\\/system_configuration/ AND objectName:\\domain_maps").

%% riak parameters
-define(BUCKET_TYPE, "cdmi").
-define(BUCKET_NAME, "cdmi").
-define(CDMI_INDEX, "cdmi_idx").
-define(NAME_PREFIX, "cdmi").

%% ACE types
-define(CDMI_ACE_ACCESS_ALLOW, 16#00000000).
-define(CDMI_ACE_ACCESS_DENY,  16#00000001).
-define(CDMI_ACE_SYSTEM_AUDIT, 16#00000002).

%% ACE type strings
-define(ACE_TYPE_STRINGS,  [
    {"ALLOW", ?CDMI_ACE_ACCESS_ALLOW},
    {"ACCESS_DENY", ?CDMI_ACE_ACCESS_DENY},
    {"SYSTEM_AUDIT", ?CDMI_ACE_SYSTEM_AUDIT}
]).

%% ACE flags
-define(CDMI_ACE_FLAGS_NONE,                  16#00000000).
-define(CDMI_ACE_FLAGS_OBJECT_INHERIT_ACE,    16#00000001).
-define(CDMI_ACE_FLAGS_CONTAINER_INHERIT_ACE, 16#00000002).
-define(CDMI_ACE_FLAGS_NO_PROPAGATE_ACE,      16#00000004).
-define(CDMI_ACE_FLAGS_INHERIT_ONLY_ACE,      16#00000008).
-define(CDMI_ACE_FLAGS_IDENTIFIER_GROUP,      16#00000040).
-define(CDMI_ACE_FLAGS_INHERITED_ACE,         16#00000080).

%% ACE type strings
-define(ACE_FLAG_STRINGS,  [
    {"NO_FLAGS", ?CDMI_ACE_FLAGS_NONE},
    {"OBJECT_INHERIT_ACE", ?CDMI_ACE_FLAGS_OBJECT_INHERIT_ACE},
    {"CONTAINER_INHERIT_ACE", ?CDMI_ACE_FLAGS_CONTAINER_INHERIT_ACE},
    {"NO_PROPAGATE_ACE", ?CDMI_ACE_FLAGS_NO_PROPAGATE_ACE},
    {"IDENTIFIER_GROUP", ?CDMI_ACE_FLAGS_IDENTIFIER_GROUP},
    {"INHERITED_ACE", ?CDMI_ACE_FLAGS_INHERITED_ACE}
]).

%% ACE masks
-define(CDMI_ACE_READ_OBJECT,                 16#00000001).
-define(CDMI_ACE_LIST_CONTAINER,              16#00000001).
-define(CDMI_ACE_WRITE_OBJECT,                16#00000002).
-define(CDMI_ACE_ADD_OBJECT,                  16#00000002).
-define(CDMI_ACE_APPEND_DATA,                 16#00000004).
-define(CDMI_ACE_ADD_SUBCONTAINER,            16#00000004).
-define(CDMI_ACE_READ_METADATA,               16#00000008).
-define(CDMI_ACE_WRITE_METADATA,              16#00000010).
-define(CDMI_ACE_EXECUTE,                     16#00000020).
-define(CDMI_ACE_TRAVERSE_CONTAINER,          16#00000020).
-define(CDMI_ACE_DELETE_OBJECT,               16#00000040).
-define(CDMI_ACE_DELETE_SUBCONTAINER,         16#00000040).
-define(CDMI_ACE_READ_ATTRIBUTES,             16#00000080).
-define(CDMI_ACE_WRITE_ATTRIBUTES,            16#00000100).
-define(CDMI_ACE_WRITE_RETENTION,             16#00000200).
-define(CDMI_ACE_WRITE_RETENTION_HOLD,        16#00000400).
-define(CDMI_ACE_DELETE,                      16#00001000).
-define(CDMI_ACE_READ_ACL,                    16#00002000).
-define(CDMI_ACE_WRITE_ACL,                   16#00040000).
-define(CDMI_ACE_WRITE_OWNER,                 16#00080000).
-define(CDMI_ACE_SYNCHRONIZE,                 16#00100000).

%% ACE mask strings
-define(ACE_MASK_STRINGS,  [
    {"READ_OBJECT", ?CDMI_ACE_READ_OBJECT},
    {"LIST_CONTAINER", ?CDMI_ACE_LIST_CONTAINER},
    {"WRITE_OBJECT",   ?CDMI_ACE_WRITE_OBJECT},
    {"ADD_OBJECT", ?CDMI_ACE_ADD_OBJECT},
    {"APPEND_DATA", ?CDMI_ACE_APPEND_DATA},
    {"ADD_SUBCONTAINER", ?CDMI_ACE_ADD_SUBCONTAINER},
    {"READ_METADATA", ?CDMI_ACE_READ_METADATA},
    {"WRITE_METADATA", ?CDMI_ACE_WRITE_METADATA,}
    {"EXECUTE", ?CDMI_ACE_EXECUTE},
    {"TRAVERSE_CONTAINER", ?CDMI_ACE_TRAVERSE_CONTAINER},
    {"DELETE_OBJECT", ?CDMI_ACE_DELETE_OBJECT},
    {"DELETE_SUBCONTAINER", ?CDMI_ACE_DELETE_SUBCONTAINER},
    {"READ_ATTRIBUTES", ?CDMI_ACE_READ_ATTRIBUTES},
    {"WRITE_ATTRIBUTES", ?CDMI_ACE_WRITE_ATTRIBUTES},
    {"WRITE_RETENTION", ?CDMI_ACE_WRITE_RETENTION},
    {"WRITE_RETENTION_HOLD", ?CDMI_ACE_WRITE_RETENTION_HOLD},
    {"DELETE", ?CDMI_ACE_DELETE},
    {"READ_ACL", ?CDMI_ACE_READ_ACL},
    {"WRITE_ACL", ?CDMI_ACE_WRITE_ACL},
    {"WRITE_OWNER", ?CDMI_ACE_WRITE_OWNER},
    {"SYNCHRONIZE", ?CDMI_ACE_SYNCHRONIZE}
]).
