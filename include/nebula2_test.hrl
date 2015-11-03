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
        \"cdmi_atime\": \"2015-11-02T20:32:56.000000Z\",
        \"cdmi_ctime\": \"2015-10-27T19:11:1445994706.000000Z\",
        \"cdmi_hash\": \"b25b294cb4deb69ea00a4c3cf3113904801b6015e5956bd019a8570b1fe1d6040e944ef3cdee16d0a46503ca6e659a25f21cf9ceddc13f352a3c98138c15d6af\",
        \"cdmi_mtime\": \"2015-10-27T19:12:14.000000Z\",
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
-define(TestBinary, <<"{\"capabilitiesURI\":\"/cdmi_capabilities/container/\",\"children\":[\"new_object1.txt\",\"multipart6.txt\",\"multipart7.txt\",\"multipart1.txt\",\"Janice-SchoolPhoto.jpg\"],\"childrenrange\":\"0-4\",\"completionStatus\":\"Complete\",\"domainURI\":\"/cdmi_domains/Fuzzcat/\",\"metadata\":{\"cdmi_acls\":[{\"aceflags\":\"OBJECT_INHERIT, CONTAINER_INHERIT\",\"acemask\":\"ALL_PERMS\",\"acetype\":\"ALLOW\",\"identifier\":\"OWNER@\"},{\"aceflags\":\"OBJECT_INHERIT, CONTAINER_INHERIT\",\"acemask\":\"READ\",\"acetype\":\"ALLOW\",\"identifier\":\"AUTHENTICATED@\"},{\"aceflags\":\"INHERITED, OBJECT_INHERIT, CONTAINER_INHERIT\",\"acemask\":\"ALL_PERMS\",\"acetype\":\"ALLOW\",\"identifier\":\"OWNER@\"},{\"aceflags\":\"INHERITED, OBJECT_INHERIT, CONTAINER_INHERIT\",\"acemask\":\"READ\",\"acetype\":\"ALLOW\",\"identifier\":\"AUTHENTICATED@\"}],\"cdmi_atime\":\"2015-10-30T20:31:22.000000Z\",\"cdmi_ctime\":\"2015-10-27T19:13:43.000000Z\",\"cdmi_domain_enabled\":\"false\",\"cdmi_mtime\":\"2015-10-27T19:13:43.000000Z\",\"cdmi_owner\":\"administrator\",\"nebula_data_location\":[\"US-TX\"]},\"objectID\":\"b8800cef188d474f801d656995a99945000452410048F52F\",\"objectName\":\"new_container7/\",\"objectType\":\"application/cdmi-container\",\"parentID\":\"0ad1801b18b14eb49708d1f9daa34fcb000452410048D534\",\"parentURI\":\"/\"}">>).
