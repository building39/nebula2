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
-type cdmi_state()       :: tuple().

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
-define(MEMCACHE_EXPIRY, 60).   %% Expire after 60 seconds.

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

%% riak parameters
-define(BUCKET_TYPE, "cdmi").
-define(BUCKET_NAME, "cdmi").
-define(CDMI_INDEX, "cdmi_idx").
-define(NAME_PREFIX, "cdmi").

