%% @author mmartin
%% @doc Handle CDMI container objects.

-module(nebula2_containers).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("nebula2_test.hrl").
-endif.

-include("nebula.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([
            new_container/2,
            update_container/3
        ]).

%% @doc Create a new CDMI container
-spec new_container(cowboy_req:req(), cdmi_state()) -> {boolean(), cowboy_req:req(), cdmi_state()}.
new_container(Req, State) when is_tuple(State) ->
%    ?nebMsg("Entry"),
    ObjectType = ?CONTENT_TYPE_CDMI_CONTAINER,
    {ok, Body, Req2} = cowboy_req:body(Req),
    Body2 = try jsx:decode(Body, [return_maps]) of
                NewBody ->
                    NewBody
            catch
                error:badarg ->
                    throw(badjson)
            end,
    case nebula2_utils:create_object(State, ObjectType, Body2) of
        {true, Data} ->
            Data2 = nebula2_db:unmarshall(Data),
            {true, cowboy_req:set_resp_body(jsx:encode(maps:to_list(Data2)), Req2), State};
        {false, _} ->
            {false, Req2, State}
    end.

%% @doc Update a CDMI container
-spec update_container(cowboy_req:req(), map(), object_oid()) -> {ok, json_value()}.
update_container(Req, State, Oid) when is_tuple(State), is_binary(Oid) ->
%    ?nebMsg("Entry"),
    {Pid, _} = State,
    {ok, Body, Req2} = cowboy_req:body(Req),
    NewData = try jsx:decode(Body, [return_maps]) of
                  NewBody -> NewBody
              catch
                  error:badarg ->
                      throw(badjson)
              end,
    {ok, OldData} = nebula2_db:read(Pid, Oid),
    OldData2 = nebula2_db:unmarshall(OldData),
    OldMetaData = nebula2_utils:get_value(<<"metadata">>, OldData2, maps:new()),
    NewMetaData = nebula2_utils:get_value(<<"metadata">>, NewData, maps:new()),
    MetaData = maps:merge(OldMetaData, NewMetaData),
    Data = maps:merge(OldData2, NewData),
    Data2 = nebula2_utils:put_value(<<"metadata">>, MetaData, Data),
    CList = [<<"cdmi_atime">>,
             <<"cdmi_mtime">>,
             <<"cdmi_acount">>,
             <<"cdmi_mcount">>],
    Data3 = nebula2_utils:update_data_system_metadata(CList, Data2, State),
    case nebula2_db:update(Pid, Oid, nebula2_db:marshall(Data3)) of
        {ok, _} ->
            {true, Req2, State};
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
nebula2_containers_test_() ->
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
     {"new_container/2",
       fun() ->
            Pid = self(),
            Req = "",
            EnvMap = maps:from_list([{<<"path">>, <<"/new_container/">>},
                                     {<<"auth_as">>, <<"MickeyMouse">>},
                                     {<<"domainURI">>, <<"/cdmi_domains/system_domain/">>}
                                    ]),
            TestMap = jsx:decode(?TestSystemConfiguration, [return_maps]),
            TestMapCDMI = maps:from_list([{<<"cdmi">>, TestMap},
                                              {<<"sp">>, ?TestCreateContainerSearchPath}
                                             ]),
            State = {Pid, EnvMap},
            meck:loop(cowboy_req, body, 1, [{ok, <<"{}">>, Req}, {ok, "bad json", Req}]),
            meck:sequence(cowboy_req, set_resp_body, 2, [Req]),
            meck:sequence(nebula2_utils, create_object, 3, [{true, TestMapCDMI}, {false, notfound}]),
            ?assertMatch({true, Req, State}, new_container(Req, State)),
            ?assertException(throw, badjson, new_container(Req, State)),
            ?assertMatch({false, Req, State}, new_container(Req, State)),
            ?assertException(error, function_clause, new_container(Req, not_a_tuple)),
            ?assert(meck:validate(cowboy_req)),
            ?assert(meck:validate(nebula2_utils))
       end
      },
      {"Test update_container/3",
       fun() ->
            Pid = self(),
            Req = "",
            EnvMap = maps:from_list([{<<"path">>, <<"/new_container/">>},
                                     {<<"auth_as">>, <<"MickeyMouse">>},
                                     {<<"domainURI">>, <<"/cdmi_domains/system_domain/">>}
                                    ]),
            TestMap = jsx:decode(?TestSystemConfiguration, [return_maps]),
            TestMapCDMI = maps:from_list([{<<"cdmi">>, TestMap},
                                              {<<"sp">>, ?TestCreateContainerSearchPath}
                                             ]),
            State = {Pid, EnvMap},
            Oid = maps:get(<<"objectID">>, TestMap),
            Body = <<"{\"metadata\": {\"new_metadata\": \"junk\"}}">>,
            Capabilities = jsx:decode(?TestSystemCapabilities, [return_maps]),
            meck:reset(cowboy_req),
            meck:reset(nebula2_db),
            meck:loop(cowboy_req, body, 1, [{ok, Body, Req}, {ok, "bad json", Req}]),
            meck:loop(nebula2_db, read, 2, [{ok, TestMapCDMI}]),
            meck:loop(nebula2_db, search, 2, [{ok, Capabilities}]),
            meck:sequence(nebula2_db, update, 3, [{ok, TestMap}, {error, notfound}]),
            ?assertMatch({true, Req, State}, update_container(Req, State, Oid)),
            ?assertException(throw, badjson, update_container(Req, State, Oid)),
            ?assertMatch({false, Req, State}, update_container(Req, State, Oid)),
            ?assertException(error, function_clause, update_container(Req, not_a_tuple, Oid)),
            ?assertException(error, function_clause, update_container(Req, State, not_a_binary)),
            ?assert(meck:validate(cowboy_req)),
            ?assert(meck:validate(nebula2_db))
        end
      }
	]
  }.
-endif.
