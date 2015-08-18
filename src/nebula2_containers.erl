%% @author mmartin
%% @doc Handle CDMI container objects.

-module(nebula2_containers).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("nebula.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([
            get_container/2,
            new_container/2,
            update_container/3
        ]).

%% @doc Get a CDMI container
-spec nebula2_containers:get_container(pid(), object_oid()) -> {ok, json_value()}.
get_container(Pid, Oid) ->
    lager:debug("Entry nebula2_containers:get_container"),
    {ok, Data} = nebula2_riak:get(Pid, Oid),
    Data.

%% @doc Create a new CDMI container
-spec nebula2_containers:new_container(Req, State) -> {boolean(), Req, State}
        when Req::cowboy_req:req().
new_container(Req, State) ->
    lager:debug("Entry nebula2_containers:new_container"),
    ObjectType = ?CONTENT_TYPE_CDMI_CONTAINER,
    DomainName = "fake domain",
    nebula2_utils:create_object(Req, State, ObjectType, DomainName).

%% @doc Update a CDMI container
-spec nebula2_containers:update_container(pid(), object_oid(), map()) -> {ok, json_value()}.
update_container(Pid, Oid, NewData) ->
    lager:debug("Entry nebula2_containers:update_container: Pid: ~p Oid: ~p NewData: ~p", [Pid, Oid, NewData]),
    {ok, OldData} = nebula2_riak:get(Pid, Oid),
    OldMetaData = maps:get(<<"metadata">>, OldData, maps:new()),
    NewMetaData = maps:get(<<"metadata">>, NewData, maps:new()),
    MetaData = maps:merge(OldMetaData, NewMetaData),
    Data = maps:merge(OldData, NewData),
    Data2 = maps:put(<<"metadata">>, MetaData, Data),
    nebula2_riak:update(Pid, Oid, Data2).

%% ====================================================================
%% Internal functions
%% ====================================================================
%% build_path(L) ->
%%     build_path(L, []).
%% build_path([], Acc) ->
%%     Acc;
%% build_path([H|T], Acc) ->
%%     Acc2 = lists:append(Acc, binary_to_list(H) ++ "/"),
%%     build_path(T, Acc2).
%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).
-endif.