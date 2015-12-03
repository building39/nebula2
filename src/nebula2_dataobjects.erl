%% @author mmartin
%% @doc Handle CDMI data objects.

-module(nebula2_dataobjects).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("nebula2_test.hrl").
-endif.

-include("nebula.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([
            get_dataobject/2,
            new_dataobject/3,
            update_dataobject/4
        ]).

%% @doc Get a CDMI dataobject
-spec nebula2_dataobjects:get_dataobject(pid(), object_oid()) -> {ok, json_value()}.
get_dataobject(Pid, Oid) when is_pid(Pid), is_binary(Oid) ->
    ?nebMsg("Entry"),
    {ok, Data} = nebula2_db:read(Pid, Oid),
    Data.

%% @doc Create a new CDMI dataobject
-spec nebula2_dataobjects:new_dataobject(cowboy_req:req(),
                                         cdmi_state(),
                                         map()) -> {boolean(), cowboy_req:req(), cdmi_state()}.
new_dataobject(Req, State, Body) when is_tuple(State), is_map(Body) ->
    ?nebMsg("Entry"),
    ObjectType = ?CONTENT_TYPE_CDMI_DATAOBJECT,
    Response = case nebula2_utils:create_object(State, ObjectType, Body) of
                   {true, Data} ->
                       Data2 = nebula2_db:unmarshall(Data),
                       {true, cowboy_req:set_resp_body(jsx:encode(maps:to_list(Data2)), Req), State};
                   false ->
                       ?nebErrMsg("Error on new object create"),
                       {false, Req, State}
               end,
    Response.

%% @doc Update a CDMI dataobject
-spec nebula2_dataobjects:update_dataobject(cowboy_req:req(),
                                            cdmi_state(),
                                            object_oid(),
                                            map()) -> {boolean(), cowboy_req:req(), cdmi_state()}.
update_dataobject(Req, State, Oid, NewData) when is_tuple(State), is_binary(Oid), is_map(NewData) ->
    ?nebMsg("Entry"),
    {Pid, _EnvMap} = State,
    nebula2_utils:check_base64(NewData),
    {ok, CdmiData} = nebula2_db:read(Pid, Oid),
    OldData = nebula2_db:unmarshall(CdmiData),
    OldMetaData = nebula2_utils:get_value(<<"metadata">>, OldData, maps:new()),
    MetaData = case maps:is_key(<<"metadata">>, NewData) of
                    true ->
                        NewMetaData = nebula2_utils:get_value(<<"metadata">>, NewData, maps:new()),
                        maps:merge(OldMetaData, NewMetaData);
                    false ->
                        OldMetaData
               end,
    Data = maps:merge(OldData, NewData),
    Data2 = nebula2_utils:put_value(<<"metadata">>, MetaData, Data),
    CList = [<<"cdmi_atime">>,
             <<"cdmi_mtime">>,
             <<"cdmi_acount">>,
             <<"cdmi_mcount">>,
             <<"cdmi_size">>],
    Data3 = nebula2_utils:update_data_system_metadata(CList, Data2, State),
    Data4 = nebula2_db:marshall(Data3),
    case nebula2_db:update(Pid, Oid, Data4) of
        {ok, _Updated} ->
            {true, Req, State};
        _  ->
            {false, Req, State}
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).
nebula2_dataobjects_test_() ->
    {foreach,
     fun() ->
             meck:new(cowboy_req, [non_strict]),
             meck:new(nebula2_db, [passthrough]),
             meck:new(nebula2_utils, [passthrough])
     end,
     fun(_) ->
             meck:unload(cowboy_req),
             meck:unload(nebula2_db),
             meck:unload(nebula2_utils)
     end,
     [
      {"get_dataobject/2",
       fun() ->
               Pid = self(),
               TestMap = jsx:decode(?TestSystemConfiguration, [return_maps]),
               Oid = maps:get(<<"objectID">>, TestMap),
               TestMapCDMI = maps:from_list([{<<"cdmi">>, TestMap},
                                              {<<"sp">>, ?TestCreateContainerSearchPath}
                                             ]),
               meck:loop(nebula2_db, read, 2, [{ok, TestMapCDMI}]),
               ?assertMatch(TestMapCDMI, get_dataobject(Pid, Oid)),
               ?assertException(error, function_clause, get_dataobject(not_a_pid, Oid)),
               ?assertException(error, function_clause, get_dataobject(Pid, not_a_binary)),
               ?assert(meck:validate(nebula2_db))
       end
      },
      {"new_dataobject/2",
       fun() ->
               Body = maps:from_list([{<<"value">>, <<"new data">>}]),
               EnvMap = maps:from_list([{<<"path">>, <<"/new_container/">>},
                                     {<<"auth_as">>, <<"MickeyMouse">>},
                                     {<<"domainURI">>, <<"/cdmi_domains/system_domain/">>}
                                    ]),
               Pid = self(),
               Req = "",
               State = {Pid, EnvMap},
               TestMap = jsx:decode(?TestSystemConfiguration, [return_maps]),
               TestMapCDMI = maps:from_list([{<<"cdmi">>, TestMap},
                                              {<<"sp">>, ?TestCreateContainerSearchPath}
                                             ]),
               meck:loop(cowboy_req, set_resp_body, 2, [Req]),
               meck:sequence(nebula2_utils, create_object, 3, [{true, TestMapCDMI}, false]),
               ?assertMatch({true, Req, State}, new_dataobject(Req, State, Body)),
               ?assertMatch({false, Req, State}, new_dataobject(Req, State, Body)),
               ?assertException(error, function_clause, new_dataobject(Req, not_a_tuple, Body)),
               ?assertException(error, function_clause, new_dataobject(Req, State, not_a_map)),
               ?assert(meck:validate(cowboy_req)),
               ?assert(meck:validate(nebula2_utils))
       end
      },
      {"update_dataobject/2",
       fun() ->
               EnvMap = maps:from_list([{<<"path">>, <<"/new_container/">>},
                                     {<<"auth_as">>, <<"MickeyMouse">>},
                                     {<<"domainURI">>, <<"/cdmi_domains/system_domain/">>}
                                    ]),
               Pid = self(),
               Req = "",
               State = {Pid, EnvMap},
               TestMap = jsx:decode(?TestSystemConfiguration, [return_maps]),
               Data = maps:from_list([{<<"value">>, <<"new data">>}]),
               Data2 = maps:from_list([{<<"metadata">>, maps:from_list([{<<"new_metadata">>, <<"data">>}])}]),
               Oid = maps:get(<<"objectID">>, TestMap),
               TestMapCDMI = maps:from_list([{<<"cdmi">>, TestMap},
                                              {<<"sp">>, ?TestCreateContainerSearchPath}
                                             ]),
               MetaData = maps:get(<<"metadata">>, maps:get(<<"cdmi">>, TestMapCDMI)),
               meck:loop(nebula2_db, read, 2, [{ok, TestMapCDMI}]),
               meck:sequence(nebula2_db, update, 3, [{ok, TestMapCDMI},
                                                     {ok, TestMapCDMI},
                                                     {error, notfound}]),
               meck:loop(nebula2_utils, update_data_system_metadata, 3, [MetaData]),
               ?assertMatch({true, Req, State }, update_dataobject(Req, State, Oid, Data)),
               ?assertMatch({true, Req, State }, update_dataobject(Req, State, Oid, Data2)),
               ?assertMatch({false, Req, State }, update_dataobject(Req, State, Oid, Data2)),
               ?assertException(error, function_clause, update_dataobject(Req, not_a_tuple, Oid, Data)),
               ?assertException(error, function_clause, update_dataobject(Req, State, not_a_binary, Data)),
               ?assertException(error, function_clause, update_dataobject(Req, State, Oid, not_a_map)),
               ?assert(meck:validate(nebula2_db)),
               ?assert(meck:validate(nebula2_utils))
       end
      }
     ]
  }.
-endif.
