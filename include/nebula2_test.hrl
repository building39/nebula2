-define(TestSearchKey, <<"c8c17baf9a68a8dbc75b818b24269ebca06b0f31/system_configuration/domain_maps">>).
-define(TestMetadataModule, nebula2_riak).
-define(TestRiakObject, {riakc_obj,
                  {<<"cdmi">>,<<"cdmi">>},
                  <<"809876fd89ac405680b7251c2e57faa30004524100486220">>,
                  <<107,206,97,96,96,96,204,96,202,5,82,60,175,244,110,152,207,205,171,58,198,192,224,87,155,193,148,200,148,199,202,208,231,177,228,60,95,22,0>>,
                  [{{dict,3,16,16,8,80,48,{[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]},{{[],[],[],[],[],[],[],[],[],[],[[<<"content-type">>,97,112,112,108,105,99,97,116,105,111,110,47,106,115,111,110],[<<"X-Riak-VTag">>,99,53,106,89,113,80,78,101,68,107,52,72,100,83,85,89,108,83,48,120,82]],[],[],[[<<"X-Riak-Last-Modified">>|{1445,973134,299471}]],[],[]}}},<<"{\"cdmi\":{\"capabilities\":{\"cdmi_RPO\":\"false\",\"cdmi_RTO\":\"false\",\"cdmi_acl\":\"true\",\"cdmi_acount\":\"false\",\"cdmi_assignedsize\":\"false\",\"cdmi_atime\":\"true\",\"cdmi_authentication_methods\":[\"anonymous\",\"basic\"],\"cdmi_copy_container\":\"false\",\"cdmi_copy_dataobject\":\"false\",\"cdmi_create_container\":\"true\",\"cdmi_create_dataobject\":\"true\",\"cdmi_create_queue\":\"false\",\"cdmi_create_reference\":\"false\",\"cdmi_create_value_range\":\"false\",\"cdmi_ctime\":\"true\",\"cdmi_data_autodelete\":\"false\",\"cdmi_data_dispersion\":\"false\",\"cdmi_data_holds\":\"false\",\"cdmi_data_redundancy\":\"\",\"cdmi_data_retention\":\"false\",\"cdmi_delete_container\":\"true\",\"cdmi_deserialize_container\":\"false\",\"cdmi_deserialize_dataobject\":\"false\",\"cdmi_deserialize_queue\":\"false\",\"cdmi_encryption\":[],\"cdmi_export_container_cifs\":\"false\",\"cdmi_export_container_iscsi\":\"false\",\"cdmi_export_container_nfs\":\"false\",\"cdmi_export_container_occi\":\"false\",\"cdmi_export_container_webdav\":\"false\",\"cdmi_geographic_placement\":\"false\",\"cdmi_immediate_redundancy\":\"\",\"cdmi_infrastructure_redundancy\":\"\",\"cdmi_latency\":\"false\",\"cdmi_list_children\":\"true\",\"cdmi_list_children_range\":\"true\",\"cdmi_mcount\":\"false\",\"cdmi_modify_deserialize_container\":\"false\",\"cdmi_modify_metadata\":\"true\",\"cdmi_move_container\":\"false\",\"cdmi_move_dataobject\":\"false\",\"cdmi_mtime\":\"true\",\"cdmi_post_dataobject\":\"false\",\"cdmi_post_queue\":\"false\",\"cdmi_read_metadata\":\"true\",\"cdmi_read_value\":\"false\",\"cdmi_read_value_range\":\"false\",\"cdmi_sanitization_method\":[],\"cdmi_serialize_container\":\"false\",\"cdmi_serialize_dataobject\":\"false\",\"cdmi_serialize_domain\":\"false\",\"cdmi_serialize_queue\":\"false\",\"cdmi_size\":\"true\",\"cdmi_snapshot\":\"false\",\"cdmi_throughput\":\"false\",\"cdmi_value_hash\":[\"MD5\",\"RIPEMD160\",\"SHA1\",\"SHA224\",\"SHA256\",\"SHA384\",\"SHA512\"]},\"children\":[\"permanent/\"],\"childrenrange\":\"0-0\",\"objectID\":\"809876fd89ac405680b7251c2e57faa30004524100486220\",\"objectName\":\"container/\",\"objectType\":\"application/cdmi-capability\",\"parentID\":\"b349d311b8404e2c86ea134b6d2eba48000452410048650D\",\"parentURI\":\"/cdmi_capabilities/\"},\"sp\":\"e1c36ee8b6b76553d8977eb4737df5b996b418bd/cdmi_capabilities/container/\"}">>}],undefined,undefined}).
-define(TestOid, <<"809876fd89ac405680b7251c2e57faa30004524100486220">>).
-define(TestContainer7Oid, <<"b8800cef188d474f801d656995a99945000452410048F52F">>).
-define(TestQuery, "sp:\\c8c17baf9a68a8dbc75b818b24269ebca06b0f31/new_container7/").
-define(TestQuery2, "sp:\\c8c17baf9a68a8dbc75b818b24269ebca06b0f31/system_configuration/domain_maps").
-define(TestSystemCapabilitiesPath, "e1c36ee8b6b76553d8977eb4737df5b996b418bd/cdmi_capabilities/dataobject/permanent/").
-define(TestUuid4, <<201,105,227,158,156,203,67,198,179,137,67,232,173,68, 104,175>>).
-define(TestUidString, "c969e39e-9ccb-43c6-b389-43e8ad4468af").
-define(TestUid, <<"c969e39e9ccb43c6b38943e8ad4468af00045241004880ea">>).
-define(TestSearchData_1_Result, {<<"score">>, <<"4.06805299999999991911e+00">>},
                                  {<<"_yz_rb">>, <<"cdmi">>},
                                  {<<"_yz_rt">>, <<"cdmi">>},
                                  {<<"_yz_rk">>, <<"809876fd89ac405680b7251c2e57faa30004524100486220">>},
                                  {<<"_yz_id">>, <<"1*cdmi*cdmi*809876fd89ac405680b7251c2e57faa30004524100486220*7">>}).

-define(TestSearchResults_0_Result, {search_results,[],0.0,0}).
-define(TestSearchResults_1_Result, {search_results,
                                     [
                                      {<<"cdmi_idx">>,
                                       [
                                        {<<"score">>,<<"4.27714499999999997470e+00">>},
                                        {<<"_yz_rb">>,<<"cdmi">>},
                                        {<<"_yz_rt">>,<<"cdmi">>},
                                        {<<"_yz_rk">>,<<"809876fd89ac405680b7251c2e57faa30004524100486220">>},
                                        {<<"_yz_id">>,<<"1*cdmi*cdmi*809876fd89ac405680b7251c2e57faa30004524100486220*9">>}
                                       ]
                                      }
                                     ],
                                     4.277144908905029,
                                     1
                                    }).
-define(TestSearchResults_2_Result, {search_results,
                                     [
                                      {<<"cdmi_idx">>,
                                       [
                                        {<<"score">>,<<"4.27714499999999997470e+00">>},
                                        {<<"_yz_rb">>,<<"cdmi">>},
                                        {<<"_yz_rt">>,<<"cdmi">>},
                                        {<<"_yz_rk">>,<<"809876fd89ac405680b7251c2e57faa30004524100486220">>},
                                        {<<"_yz_id">>,<<"1*cdmi*cdmi*809876fd89ac405680b7251c2e57faa30004524100486220*9">>}
                                       ]
                                      }
                                     ],
                                     4.277144908905029,
                                     2
                                    }).
-define(GET_ENV(X, Y), {ok, ?TestMetadataModule}).
-define(TestDomainMapsValue, #{<<"(cloud)[.]fuzzcat[.]net$">> => <<"Fuzzcat/">>}).
-define(TestSystemDomainHash, "c8c17baf9a68a8dbc75b818b24269ebca06b0f31").
-define(TestDomainMaps, <<"{
    \"capabilitiesURI\": \"/cdmi_capabilities/dataobject/permanent/\",
    \"completionStatus\": \"complete\",
    \"domainURI\": \"/cdmi_domains/system_domain/\",
    \"metadata\": {
        \"cdmi_acls\": [
            {
                \"aceflags\": \"OBJECT_INHERIT, CONTAINER_INHERIT\",
                \"acemask\": \"ALL_PERMS\",
                \"acetype\": \"ALLOW\",
                \"identifier\": \"OWNER@\"
            },
            {
                \"aceflags\": \"OBJECT_INHERIT, CONTAINER_INHERIT\",
                \"acemask\": \"READ\",
                \"acetype\": \"ALLOW\",
                \"identifier\": \"AUTHENTICATED@\"
            },
            {
                \"aceflags\": \"INHERITED, OBJECT_INHERIT, CONTAINER_INHERIT\",
                \"acemask\": \"ALL_PERMS\",
                \"acetype\": \"ALLOW\",
                \"identifier\": \"OWNER@\"
            },
            {
                \"aceflags\": \"INHERITED, OBJECT_INHERIT, CONTAINER_INHERIT\",
                \"acemask\": \"READ\",
                \"acetype\": \"ALLOW\",
                \"identifier\": \"AUTHENTICATED@\"
            }
        ],
        \"cdmi_atime\": \"1970-01-01T00:00:00.000000Z\",
        \"cdmi_ctime\": \"1970-01-01T00:00:00.000000Z\",
        \"cdmi_hash\": \"b25b294cb4deb69ea00a4c3cf3113904801b6015e5956bd019a8570b1fe1d6040e944ef3cdee16d0a46503ca6e659a25f21cf9ceddc13f352a3c98138c15d6af\",
        \"cdmi_mtime\": \"1970-01-01T00:00:00.000000Z\",
        \"cdmi_owner\": \"administrator\",
        \"cdmi_size\": 1,
        \"cdmi_value_hash\": \"SHA512\"
    },
    \"objectID\": \"6e41cde8dce74ee6912164e1f09ae3240004524100484F3F\",
    \"objectName\": \"domain_maps\",
    \"objectType\": \"application/cdmi-object\",
    \"parentID\": \"360d1572eccb4d44b57386d63576dbc400045241004850EF\",
    \"parentURI\": \"/system_configuration/\",
    \"value\": [
        {
            \"(cloud)[.]fuzzcat[.]net$\": \"Fuzzcat/\"
        }
    ],
    \"valuerange\": \"0-1\",
    \"valuetransferencoding\": \"utf-8\"
}">>).
-define(TestSystemConfiguration, <<"{
    \"capabilitiesURI\": \"/cdmi_capabilities/container/permanent/\",
    \"children\": [
        \"environment_variables/\",
        \"domain_maps\"
    ],
    \"childrenrange\": \"0-1\",
    \"completionStatus\": \"complete\",
    \"domainURI\": \"/cdmi_domains/system_domain/\",
    \"metadata\": {
        \"cdmi_acls\": [
            {
                \"aceflags\": \"OBJECT_INHERIT, CONTAINER_INHERIT\",
                \"acemask\": \"ALL_PERMS\",
                \"acetype\": \"ALLOW\",
                \"identifier\": \"OWNER@\"
            },
            {
                \"aceflags\": \"OBJECT_INHERIT, CONTAINER_INHERIT\",
                \"acemask\": \"READ\",
                \"acetype\": \"ALLOW\",
                \"identifier\": \"AUTHENTICATED@\"
            },
            {
                \"aceflags\": \"INHERITED, OBJECT_INHERIT, CONTAINER_INHERIT\",
                \"acemask\": \"ALL_PERMS\",
                \"acetype\": \"ALLOW\",
                \"identifier\": \"OWNER@\"
            },
            {
                \"aceflags\": \"INHERITED, OBJECT_INHERIT, CONTAINER_INHERIT\",
                \"acemask\": \"READ\",
                \"acetype\": \"ALLOW\",
                \"identifier\": \"AUTHENTICATED@\"
            }
        ],
        \"cdmi_atime\": \"1970-01-01T00:00:00.000000Z\",
        \"cdmi_ctime\": \"1970-01-01T00:00:00.000000Z\",
        \"cdmi_mtime\": \"1970-01-01T00:00:00.000000Z\",
        \"cdmi_owner\": \"administrator\"
    },
    \"objectID\": \"360d1572eccb4d44b57386d63576dbc400045241004850EF\",
    \"objectName\": \"system_configuration/\",
    \"objectType\": \"application/cdmi-container\",
    \"parentID\": \"dd37dd9ad7e341fd852f46f94deae2bf0004524100482095\",
    \"parentURI\": \"/\"
}">>).
-define(TestSystemCapabilities, <<"{
    \"capabilities\": {
        \"cdmi_acount\": \"true\",
        \"cdmi_copy_dataobject_from_queue\": \"false\",
        \"cdmi_copy_queue_by_ID\": \"false\",
        \"cdmi_create_reference_by_ID\": \"false\",
        \"cdmi_create_value_range_by_ID\": \"false\",
        \"cdmi_dataobjects\": \"true\",
        \"cdmi_deserialize_dataobject_by_ID\": \"false\",
        \"cdmi_deserialize_queue_by_ID\": \"false\",
        \"cdmi_domains\": \"false\",
        \"cdmi_export_cifs\": \"false\",
        \"cdmi_export_iscsi\": \"false\",
        \"cdmi_export_nfs\": \"false\",
        \"cdmi_export_occi_iscsi\": \"false\",
        \"cdmi_export_webdav\": \"false\",
        \"cdmi_logging\": \"false\",
        \"cdmi_mcount\": \"true\",
        \"cdmi_metadata_maxitems\": 1024,
        \"cdmi_metadata_maxsize\": 8192,
        \"cdmi_metadata_maxtotalsize\": 8388608,
        \"cdmi_multipart_mime\": \"true\",
        \"cdmi_notification\": \"false\",
        \"cdmi_object_access_by_ID\": \"true\",
        \"cdmi_object_copy_from_local\": \"false\",
        \"cdmi_object_copy_from_remote\": \"false\",
        \"cdmi_object_move_from_ID\": \"false\",
        \"cdmi_object_move_from_local\": \"false\",
        \"cdmi_object_move_from_remote\": \"false\",
        \"cdmi_object_move_to_ID\": \"false\",
        \"cdmi_post_dataobject_by_ID\": \"false\",
        \"cdmi_post_queue_by_ID\": \"false\",
        \"cdmi_query\": \"false\",
        \"cdmi_query_contains\": \"false\",
        \"cdmi_query_regex\": \"false\",
        \"cdmi_query_tags\": \"false\",
        \"cdmi_query_value\": \"false\",
        \"cdmi_queues\": \"false\",
        \"cdmi_references\": \"false\",
        \"cdmi_security_access_control\": \"false\",
        \"cdmi_security_audit\": \"false\",
        \"cdmi_security_data_integrity\": \"true\",
        \"cdmi_security_immutability\": \"false\",
        \"cdmi_security_sanitization\": \"false\",
        \"cdmi_serialization_json\": \"false\",
        \"cdmi_serialize_container_ID\": \"false\",
        \"cdmi_serialize_dataobject_to_ID\": \"false\",
        \"cdmi_serialize_domain_to_ID\": \"false\",
        \"cdmi_serialize_queue_to_ID\": \"false\",
        \"cdmi_snapshots\": \"false\",
        \"unknown_handler\": \"true\"
    },
    \"children\": [
        \"container/\",
        \"dataobject/\",
        \"domain/\"
    ],\"childrenrange\": \"0-2\",
    \"metadata\": {},
    \"objectID\": \"b349d311b8404e2c86ea134b6d2eba48000452410048650D\",
    \"objectName\": \"cdmi_capabilities/\",
    \"objectType\": \"application/cdmi-capability\",
    \"parentID\": \"dd37dd9ad7e341fd852f46f94deae2bf0004524100482095\",
    \"parentURI\": \"/\"}">>).
-define(TestCreateContainerSearchPath, <<"c8c17baf9a68a8dbc75b818b24269ebca06b0f31/new_container/">>).
-define(TestCreateContainer, <<"{
    \"capabilitiesURI\": \"/cdmi_capabilities/container/\",
    \"completionStatus\": \"Complete\",
    \"domainURI\": \"/cdmi_domains/system_domain/\",
    \"metadata\": {
        \"cdmi_acls\": [
            {
                \"aceflags\": \"OBJECT_INHERIT, CONTAINER_INHERIT\",
                \"acemask\": \"ALL_PERMS\",
                \"acetype\": \"ALLOW\",
                \"identifier\": \"OWNER@\"
            },
            {
                \"aceflags\": \"OBJECT_INHERIT, CONTAINER_INHERIT\",
                \"acemask\": \"READ\",
                \"acetype\": \"ALLOW\",
                \"identifier\": \"AUTHENTICATED@\"
            },
        ],
        \"cdmi_atime\": \"1970-01-01T00:00:00.000000Z\",
        \"cdmi_ctime\": \"1970-01-01T00:00:00.000000Z\",
        \"cdmi_mtime\": \"1970-01-01T00:00:00.000000Z\",
        \"my_metadata\": \"junk\",
        \"cdmi_owner\": \"MickeyMouse\",
        \"nebula_data_location\": [
            \"US-TX\"
        ]
    },
    \"objectID\": \"c969e39e9ccb43c6b38943e8ad4468af00045241004880ea\",
    \"objectName\": \"new_container/\",
    \"objectType\": \"application/cdmi-container\",
    \"parentID\": \"dd37dd9ad7e341fd852f46f94deae2bf0004524100482095\",
    \"parentURI\": \"/\"
}">>).
-define(TestRootObject, <<"{
    \"capabilitiesURI\": \"/cdmi_capabilities/\",
    \"children\": [
        \"cdmi_domains/\",
        \"system_configuration/\",
        \"cdmi_capabilities/\",
        \"new_container7/\",
        \"new_container2/\",
        \"new_container3/\",
        \"new_container5/\",
        \"new_containerz/\",
        \"new_containerx/\",
        \"new_containery/\"
    ],
    \"childrenrange\": \"0-9\",
    \"completionStatus\": \"complete\",
    \"domainURI\": \"/cdmi_domains/system_domain/\",
    \"metadata\": {
        \"cdmi_acls\": [
            {
                \"aceflags\": \"OBJECT_INHERIT, CONTAINER_INHERIT\",
                \"acemask\": \"ALL_PERMS\",
                \"acetype\": \"ALLOW\",
                \"identifier\": \"OWNER@\"
            },
            {
                \"aceflags\": \"OBJECT_INHERIT, CONTAINER_INHERIT\",
                \"acemask\": \"READ\",
                \"acetype\": \"ALLOW\",
                \"identifier\": \"AUTHENTICATED@\"
            }
        ],
        \"cdmi_atime\": \"1970-01-01T00:00:00.000000Z\",
        \"cdmi_ctime\": \"1970-01-01T00:00:00.000000Z\",
        \"cdmi_mtime\": \"1970-01-01T00:00:00.000000Z\",
        \"cdmi_owner\": \"administrator\"
    },
    \"objectID\": \"dd37dd9ad7e341fd852f46f94deae2bf0004524100482095\",
    \"objectName\": \"/\",
    \"objectType\": \"application/cdmi-container\"
}">>).
-define(TestBinary, <<"{\"capabilitiesURI\":\"/cdmi_capabilities/container/\",\"children\":[\"new_object1.txt\",\"multipart6.txt\",\"multipart7.txt\",\"multipart1.txt\",\"Janice-SchoolPhoto.jpg\"],\"childrenrange\":\"0-4\",\"completionStatus\":\"Complete\",\"domainURI\":\"/cdmi_domains/Fuzzcat/\",\"metadata\":{\"cdmi_acls\":[{\"aceflags\":\"OBJECT_INHERIT, CONTAINER_INHERIT\",\"acemask\":\"ALL_PERMS\",\"acetype\":\"ALLOW\",\"identifier\":\"OWNER@\"},{\"aceflags\":\"OBJECT_INHERIT, CONTAINER_INHERIT\",\"acemask\":\"READ\",\"acetype\":\"ALLOW\",\"identifier\":\"AUTHENTICATED@\"},{\"aceflags\":\"INHERITED, OBJECT_INHERIT, CONTAINER_INHERIT\",\"acemask\":\"ALL_PERMS\",\"acetype\":\"ALLOW\",\"identifier\":\"OWNER@\"},{\"aceflags\":\"INHERITED, OBJECT_INHERIT, CONTAINER_INHERIT\",\"acemask\":\"READ\",\"acetype\":\"ALLOW\",\"identifier\":\"AUTHENTICATED@\"}],\"cdmi_atime\":\"1970-01-01T00:00:00.000000Z\",\"cdmi_ctime\":\"1970-01-01T00:00:00.000000Z\",\"cdmi_domain_enabled\":\"false\",\"cdmi_mtime\":\"1970-01-01T00:00:00.000000Z\",\"cdmi_owner\":\"administrator\",\"nebula_data_location\":[\"US-TX\"]},\"objectID\":\"b8800cef188d474f801d656995a99945000452410048F52F\",\"objectName\":\"new_container7/\",\"objectType\":\"application/cdmi-container\",\"parentID\":\"0ad1801b18b14eb49708d1f9daa34fcb000452410048D534\",\"parentURI\":\"/\"}">>).
-define(TestSystemDomain, <<"{
    \"capabilitiesURI\": \"/cdmi_capabilities/domain/\",
    \"children\": [
        \"cdmi_domain_members/\",
        \"cdmi_domain_summary/\"
    ],
    \"childrenrange\": \"0-1\",
    \"completionStatus\": \"complete\",
    \"domainURI\": \"/cdmi_domains/system_domain/\",
    \"metadata\": {
        \"cdmi_acls\": [
            {
                \"aceflags\": \"OBJECT_INHERIT, CONTAINER_INHERIT\",
                \"acemask\": \"ALL_PERMS\",
                \"acetype\": \"ALLOW\",
                \"identifier\": \"OWNER@\"
            },
            {
                \"aceflags\": \"OBJECT_INHERIT, CONTAINER_INHERIT\",
                \"acemask\": \"READ\",
                \"acetype\": \"ALLOW\",
                \"identifier\": \"AUTHENTICATED@\"
            },
            {
                \"aceflags\": \"INHERITED, OBJECT_INHERIT, CONTAINER_INHERIT\",
                \"acemask\": \"ALL_PERMS\",
                \"acetype\": \"ALLOW\",
                \"identifier\": \"OWNER@\"
            },
            {
                \"aceflags\": \"INHERITED, OBJECT_INHERIT, CONTAINER_INHERIT\",
                \"acemask\": \"READ\",
                \"acetype\": \"ALLOW\",
                \"identifier\": \"AUTHENTICATED@\"
            }
        ],
        \"cdmi_atime\": \"1970-01-01T00:00:00.000000Z\",
        \"cdmi_ctime\": \"1970-01-01T00:00:00.000000Z\",
        \"cdmi_mtime\": \"1970-01-01T00:00:00.000000Z\",
        \"cdmi_owner\": \"administrator\"
    },
    \"objectID\": \"8bd23741efb74fd6af4f2aed27aa9fb900045241004893AB\",
    \"objectName\": \"system_domain/\",
    \"objectType\": \"application/cdmi-domain\",
    \"parentID\": \"bb52a5e1e8b14b55a982c8c763317992000452410048C8B3\",
    \"parentURI\": \"/cdmi_domains/\"
}">>).
