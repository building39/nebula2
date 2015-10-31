%% @author mmartin
%% @doc @todo Add description to nebula2_riak.

-module(nebula2_riak).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("nebula2_test.hrl").
-endif.

-include_lib("riakc/include/riakc.hrl").
-include("nebula.hrl").

%% riak parameters
-define(BUCKET_TYPE, "cdmi").
-define(BUCKET_NAME, "cdmi").
-define(CDMI_INDEX, "cdmi_idx").
-define(NAME_PREFIX, "cdmi").
%% Domain Maps Query

%% ====================================================================
%% API functions
%% ====================================================================
-export([delete/2,
         get/2,
         get_domain_maps/2,
         put/3,
         available/1,
         search/2,
         update/3]).

%% @doc Ping the riak cluster.
-spec nebula2_riak:available(pid()) -> boolean().
available(Pid) when is_pid(Pid) ->
%    lager:debug("Entry"),
    case riakc_pb_socket:ping(Pid) of
        pong ->
                true;
        _R ->
             false
    end.

%% @doc Delete an object from riak by bucket type, bucket and key.
-spec nebula2_riak:delete(pid(), object_oid()) -> ok | {error, term()}.
delete(Pid, Oid) when is_binary(Oid) ->
    delete(Pid, binary_to_list(Oid));
delete(Pid, Oid) ->
    lager:debug("Entry"),
    riakc_pb_socket:delete(Pid,
                           {list_to_binary(?BUCKET_TYPE),
                            list_to_binary(?BUCKET_NAME)},
                           Oid).

%% @doc Get a value from riak by bucket type, bucket and key. Return string.
-spec nebula2_riak:get(pid(), object_oid()) -> {ok, map()}|{error, term()}.
get(Pid, Oid) when is_binary(Oid) ->
%%    lager:debug("Entry"),
    ?debugFmt("Oid: ~p", [Oid]),
    case riakc_pb_socket:get(Pid, {list_to_binary(?BUCKET_TYPE),
                                   list_to_binary(?BUCKET_NAME)},
                                   Oid) of
                {ok, Object} ->
                    ?debugFmt("Object: ~p", [Object]),
                    X = riakc_obj:get_value(Object),
                    ?debugFmt("X: ~p", [X]),
                    Data = jsx:decode(X, [return_maps]),
                    ?debugFmt("Data: ~p", [Data]),
                    {ok, Data};
                {error, Term} ->
                    {error, Term}
    end.

%% @doc Get the domain maps.
-spec nebula2_riak:get_domain_maps(pid(), object_path()) -> binary().
get_domain_maps(Pid, Path) ->
    lager:debug("Entry"),
    execute_search(Pid, "sp:\\" ++ Path).

%% @doc Put a value with content type to riak by bucket type, bucket and key. 
-spec nebula2_riak:put(pid(),
                       object_oid(),   %% Oid
                       map()           %% Data to store
                      ) -> {'error', _} | {'ok', _}.
put(Pid, Oid, Data) ->
    lager:debug("Entry"),
    do_put(Pid, Oid, Data).

-spec nebula2_riak:do_put(pid(), object_oid(), map()) -> {ok|error, object_oid()|term()}.
do_put(Pid, Oid, Data) ->
    lager:debug("Entry"),
    Json = jsx:encode(Data),
    Object = riakc_obj:new({list_to_binary(?BUCKET_TYPE),
                            list_to_binary(?BUCKET_NAME)},
                            Oid,
                            Json,
                            list_to_binary("application/json")),
    case riakc_pb_socket:put(Pid, Object) of
        ok ->
            {ok, Oid};
        {error, Term} ->
            {error, Term}
    end.

%% @doc Search an index for objects.
-spec nebula2_riak:search(string(), cdmi_state()) -> {error, 404|500}|{ok, map()}.
search(Path, State) when is_binary(Path)->
    Path2 = binary_to_list(Path),
    search(Path2, State);
search(Path, State) ->
    lager:debug("Entry"),
    {Pid, _} = State,
    Query = "sp:\\" ++ Path,
    Result =  execute_search(Pid, Query),
    Result.

%% @doc Update an existing key/value pair.
-spec nebula2_riak:update(pid(),
                          object_oid(),      %% Oid
                          map()              %% Data to store
                         ) -> ok | {error, term()}.
update(Pid, Oid, Data) when is_binary(Oid) ->
    update(Pid, binary_to_list(Oid), Data);
update(Pid, Oid, Data) ->
    lager:debug("Entry"),
    case get(Pid, Oid) of
        {error, E} ->
            {error, E};
        {ok, _} ->
            {ok, Obj} = riakc_pb_socket:get(Pid, 
                                            {list_to_binary(?BUCKET_TYPE),
                                             list_to_binary(?BUCKET_NAME)},
                                            Oid),
            NewObj = riakc_obj:update_value(Obj, Data),
            riakc_pb_socket:put(Pid, NewObj)
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @doc Execute a search.
-spec nebula2_riak:execute_search(pid(),              %% Riak client pid.
                                  search_predicate()  %% URI.
                                 ) -> {error, 404|500} |{ok, map()}.
execute_search(Pid, Query) ->
    lager:debug("Entry"),
    Index = list_to_binary(?CDMI_INDEX),
    lager:debug("Query: ~p", [Query]),
    Response = case riakc_pb_socket:search(Pid, Index, Query) of
                   {ok, Results} ->
                       case Results#search_results.num_found of
                           0 ->
                               lager:debug("Not Found"),
                               {error, 404}; %% Return 404
                           1 ->
                               [{_, Doc}] = Results#search_results.docs,
                               lager:debug("Doc: ~p", [Doc]),
                               fetch(Pid, Doc);
                           _N ->
                               lager:debug("Something funky"),
                               {error, 500} %% Something's funky - return 500
                       end;
                   _ ->
                       lager:debug("WTF?"),
                       {error, 404}
               end,
    Response.

%% @doc Fetch document.
-spec nebula2_riak:fetch(pid(), list()) -> {ok, map()}.
fetch(Pid, Data) ->
%%     lager:debug("Entry"),
    Oid = proplists:get_value(<<"_yz_rk">>, Data),
    ?debugFmt("Oid: ~p", [Oid]),
    Response = get(Pid, Oid),
    ?debugFmt("Response: ~p", [Response]),
    Response.

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).

available_test() ->
    meck:new(riakc_pb_socket, [non_strict]),
    Pid = self(),
    meck:expect(riakc_pb_socket, ping, [Pid], pong),
    ?assertMatch(true, nebula2_riak:available(Pid)),
    meck:expect(riakc_pb_socket, ping, [Pid], pang),
    ?assertMatch(false, nebula2_riak:available(Pid)).
%%    meck:unload(riakc_pb_socket).
fetch_test() ->
    Pid = self(),
    TestBinary = <<"{\"capabilitiesURI\":\"/cdmi_capabilities/container/\",\"children\":[\"new_object1.txt\",\"multipart6.txt\",\"multipart7.txt\",\"multipart1.txt\",\"Janice-SchoolPhoto.jpg\"],\"childrenrange\":\"0-4\",\"completionStatus\":\"Complete\",\"domainURI\":\"/cdmi_domains/Fuzzcat/\",\"metadata\":{\"cdmi_acls\":[{\"aceflags\":\"OBJECT_INHERIT, CONTAINER_INHERIT\",\"acemask\":\"ALL_PERMS\",\"acetype\":\"ALLOW\",\"identifier\":\"OWNER@\"},{\"aceflags\":\"OBJECT_INHERIT, CONTAINER_INHERIT\",\"acemask\":\"READ\",\"acetype\":\"ALLOW\",\"identifier\":\"AUTHENTICATED@\"},{\"aceflags\":\"INHERITED, OBJECT_INHERIT, CONTAINER_INHERIT\",\"acemask\":\"ALL_PERMS\",\"acetype\":\"ALLOW\",\"identifier\":\"OWNER@\"},{\"aceflags\":\"INHERITED, OBJECT_INHERIT, CONTAINER_INHERIT\",\"acemask\":\"READ\",\"acetype\":\"ALLOW\",\"identifier\":\"AUTHENTICATED@\"}],\"cdmi_atime\":\"2015-10-30T20:31:22.000000Z\",\"cdmi_ctime\":\"2015-10-27T19:13:43.000000Z\",\"cdmi_domain_enabled\":\"false\",\"cdmi_mtime\":\"2015-10-27T19:13:43.000000Z\",\"cdmi_owner\":\"administrator\",\"nebula_data_location\":[\"US-TX\"]},\"objectID\":\"b8800cef188d474f801d656995a99945000452410048F52F\",\"objectName\":\"new_container7/\",\"objectType\":\"application/cdmi-container\",\"parentID\":\"0ad1801b18b14eb49708d1f9daa34fcb000452410048D534\",\"parentURI\":\"/\"}">>,
    TestData = [{<<"score">>, <<"4.06805299999999991911e+00">>},
                   {<<"_yz_rb">>, <<"cdmi">>},
                   {<<"_yz_rt">>, <<"cdmi">>},
                   {<<"_yz_rk">>, <<"809876fd89ac405680b7251c2e57faa30004524100486220">>},
                   {<<"_yz_id">>, <<"1*cdmi*cdmi*809876fd89ac405680b7251c2e57faa30004524100486220*7">>}],
    TestOid = <<"809876fd89ac405680b7251c2e57faa30004524100486220">>,
    TestObject = {riakc_obj,
                  {<<"cdmi">>,<<"cdmi">>},
                  <<"809876fd89ac405680b7251c2e57faa30004524100486220">>,
                  <<107,206,97,96,96,96,204,96,202,5,82,60,175,244,110,152,207,205,171,58,198,192,224,87,155,193,148,200,148,199,202,208,231,177,228,60,95,22,0>>,
                  [{{dict,3,16,16,8,80,48,{[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]},{{[],[],[],[],[],[],[],[],[],[],[[<<"content-type">>,97,112,112,108,105,99,97,116,105,111,110,47,106,115,111,110],[<<"X-Riak-VTag">>,99,53,106,89,113,80,78,101,68,107,52,72,100,83,85,89,108,83,48,120,82]],[],[],[[<<"X-Riak-Last-Modified">>|{1445,973134,299471}]],[],[]}}},<<"{\"cdmi\":{\"capabilities\":{\"cdmi_RPO\":\"false\",\"cdmi_RTO\":\"false\",\"cdmi_acl\":\"true\",\"cdmi_acount\":\"false\",\"cdmi_assignedsize\":\"false\",\"cdmi_atime\":\"true\",\"cdmi_authentication_methods\":[\"anonymous\",\"basic\"],\"cdmi_copy_container\":\"false\",\"cdmi_copy_dataobject\":\"false\",\"cdmi_create_container\":\"true\",\"cdmi_create_dataobject\":\"true\",\"cdmi_create_queue\":\"false\",\"cdmi_create_reference\":\"false\",\"cdmi_create_value_range\":\"false\",\"cdmi_ctime\":\"true\",\"cdmi_data_autodelete\":\"false\",\"cdmi_data_dispersion\":\"false\",\"cdmi_data_holds\":\"false\",\"cdmi_data_redundancy\":\"\",\"cdmi_data_retention\":\"false\",\"cdmi_delete_container\":\"true\",\"cdmi_deserialize_container\":\"false\",\"cdmi_deserialize_dataobject\":\"false\",\"cdmi_deserialize_queue\":\"false\",\"cdmi_encryption\":[],\"cdmi_export_container_cifs\":\"false\",\"cdmi_export_container_iscsi\":\"false\",\"cdmi_export_container_nfs\":\"false\",\"cdmi_export_container_occi\":\"false\",\"cdmi_export_container_webdav\":\"false\",\"cdmi_geographic_placement\":\"false\",\"cdmi_immediate_redundancy\":\"\",\"cdmi_infrastructure_redundancy\":\"\",\"cdmi_latency\":\"false\",\"cdmi_list_children\":\"true\",\"cdmi_list_children_range\":\"true\",\"cdmi_mcount\":\"false\",\"cdmi_modify_deserialize_container\":\"false\",\"cdmi_modify_metadata\":\"true\",\"cdmi_move_container\":\"false\",\"cdmi_move_dataobject\":\"false\",\"cdmi_mtime\":\"true\",\"cdmi_post_dataobject\":\"false\",\"cdmi_post_queue\":\"false\",\"cdmi_read_metadata\":\"true\",\"cdmi_read_value\":\"false\",\"cdmi_read_value_range\":\"false\",\"cdmi_sanitization_method\":[],\"cdmi_serialize_container\":\"false\",\"cdmi_serialize_dataobject\":\"false\",\"cdmi_serialize_domain\":\"false\",\"cdmi_serialize_queue\":\"false\",\"cdmi_size\":\"true\",\"cdmi_snapshot\":\"false\",\"cdmi_throughput\":\"false\",\"cdmi_value_hash\":[\"MD5\",\"RIPEMD160\",\"SHA1\",\"SHA224\",\"SHA256\",\"SHA384\",\"SHA512\"]},\"children\":[\"permanent/\"],\"childrenrange\":\"0-0\",\"objectID\":\"809876fd89ac405680b7251c2e57faa30004524100486220\",\"objectName\":\"container/\",\"objectType\":\"application/cdmi-capability\",\"parentID\":\"b349d311b8404e2c86ea134b6d2eba48000452410048650D\",\"parentURI\":\"/cdmi_capabilities/\"},\"sp\":\"e1c36ee8b6b76553d8977eb4737df5b996b418bd/cdmi_capabilities/container/\"}">>}],undefined,undefined},
    TestMap = #{<<"cdmi">> => #{<<"capabilitiesURI">> => <<"/cdmi_capabilities/container/">>,
                               <<"children">> => [<<"new_object1.txt">>,
                                                  <<"multipart6.txt">>,
                                                  <<"multipart7.txt">>,
                                                  <<"multipart1.txt">>,
                                                  <<"Janice-SchoolPhoto.jpg">>],
                               <<"childrenrange">> => <<"0-4">>,
                               <<"completionStatus">> => <<"Complete">>,
                               <<"domainURI">> => <<"/cdmi_domains/Fuzzcat/">>,
                               <<"metadata">> => #{<<"cdmi_acls">> => [#{<<"aceflags">> => <<"OBJECT_INHERIT, CONTAINER_INHERIT">>,
                                                                         <<"acemask">> => <<"ALL_PERMS">>,
                                                                         <<"acetype">> => <<"ALLOW">>,
                                                                         <<"identifier">> => <<"OWNER@">>},
                                                                       #{<<"aceflags">> => <<"OBJECT_INHERIT, CONTAINER_INHERIT">>,
                                                                         <<"acemask">> => <<"READ">>,
                                                                         <<"acetype">> => <<"ALLOW">>,
                                                                         <<"identifier">> => <<"AUTHENTICATED@">>},
                                                                       #{<<"aceflags">> => <<"INHERITED, OBJECT_INHERIT, CONTAINER_INHERIT">>,
                                                                         <<"acemask">> => <<"ALL_PERMS">>,
                                                                         <<"acetype">> => <<"ALLOW">>,
                                                                         <<"identifier">> => <<"OWNER@">>},
                                                                       #{<<"aceflags">> => <<"INHERITED, OBJECT_INHERIT, CONTAINER_INHERIT">>,
                                                                         <<"acemask">> => <<"READ">>,
                                                                         <<"acetype">> => <<"ALLOW">>,
                                                                         <<"identifier">> => <<"AUTHENTICATED@">>}],
                                                   <<"cdmi_atime">> => <<"2015-10-30T20:31:22.000000Z">>,
                                                   <<"cdmi_ctime">> => <<"2015-10-27T19:13:43.000000Z">>,
                                                   <<"cdmi_domain_enabled">> => <<"false">>,
                                                   <<"cdmi_mtime">> => <<"2015-10-27T19:13:43.000000Z">>,
                                                   <<"cdmi_owner">> => <<"administrator">>,
                                                   <<"nebula_data_location">> => [<<"US-TX">>]},
                               <<"objectID">> => <<"b8800cef188d474f801d656995a99945000452410048F52F">>,
                               <<"objectName">> => <<"new_container7/">>,
                               <<"objectType">> => <<"application/cdmi-container">>,
                               <<"parentID">> => <<"0ad1801b18b14eb49708d1f9daa34fcb000452410048D534">>,
                               <<"parentURI">> => <<"/">>},
               <<"sp">> => <<"e2f450d94cb7f21e3596f8b953b3ec2791343482/new_container7/">>},
%%    meck:new(riakc_pb_socket, [non_strict]),
    meck:new(riakc_obj, [non_strict]),
    meck:expect(riakc_pb_socket, get, [Pid, {<<"cdmi">>, <<"cdmi">>}, TestOid], {ok, TestObject}),
    meck:expect(riakc_obj, get_value, [TestObject], TestBinary),
    {Atom, CdmiMap} = fetch(Pid, TestData),
    ?debugFmt("CdmiMap: ~p", [CdmiMap]),
    ?debugFmt("TestMap: ~p", [maps:get(<<"cdmi">>, TestMap)]),
   % Map = maps:get(<<"cdmi">>, TestMap),
    ?assert(Atom == ok),
    ?assert(CdmiMap == maps:get(<<"cdmi">>, TestMap)).
    %% ?assertMatch({ok, TestMap}, Got).
    
-endif.
