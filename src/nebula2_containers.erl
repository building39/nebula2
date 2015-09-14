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
            new_container/2,
            update_container/3
        ]).

%% @doc Create a new CDMI container
-spec nebula2_containers:new_container(Req, cdmi_state()) -> {boolean(), Req, cdmi_state()}
        when Req::cowboy_req:req().
new_container(Req, State) ->
    ObjectType = ?CONTENT_TYPE_CDMI_CONTAINER,
    DomainName = "fake domain",
    nebula2_utils:create_object(Req, State, ObjectType, DomainName).

%% @doc Update a CDMI container
-spec nebula2_containers:update_container(pid(), object_oid(), map()) -> {ok, json_value()}.
update_container(Pid, Oid, NewData) ->
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
