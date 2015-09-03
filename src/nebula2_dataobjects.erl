%% @author mmartin
%% @doc Handle CDMI data objects.

-module(nebula2_dataobjects).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("nebula.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([
            get_dataobject/2,
            new_dataobject/2,
            update_dataobject/3
        ]).

%% @doc Get a CDMI dataobject
-spec nebula2_dataobjects:get_dataobject(pid(), object_oid()) -> {ok, json_value()}.
get_dataobject(Pid, Oid) ->
    ?LOG_ENTRY,
    {ok, Data} = nebula2_riak:get(Pid, Oid),
    Data.

%% @doc Create a new CDMI dataobject
-spec nebula2_dataobjects:new_dataobject(cowboy_req:req(), cdmi_state()) -> {boolean(), cowboy_req:req(), cdmi_state()}.
new_dataobject(Req, State) ->
    ?LOG_ENTRY,
    ObjectType = ?CONTENT_TYPE_CDMI_DATAOBJECT,
    DomainName = "fake domain",
    nebula2_utils:create_object(Req, State, ObjectType, DomainName).

%% @doc Update a CDMI dataobject
-spec nebula2_dataobjects:update_dataobject(pid(), object_oid(), map()) -> {ok, json_value()}.
update_dataobject(Pid, Oid, NewData) ->
    ?LOG_ENTRY,
    {ok, OldData} = nebula2_riak:get(Pid, Oid),
    OldMetaData = maps:get(<<"metadata">>, OldData, maps:new()),
    NewMetaData = maps:get(<<"metadata">>, NewData, maps:new()),
    MetaData = maps:merge(OldMetaData, NewMetaData),
    Data = maps:merge(OldData, NewData),
    Data2 = maps:put(<<"metadata">>, MetaData, Data),
    Value = binary_to_list(jsx:encode(maps:get(<<"value">>, Data2))),
    Length = length(Value) - 1,
    ValueRange = list_to_binary(lists:flatten(io_lib:format("0-~p", [Length]))),
    Data3 = maps:put(<<"valuerange">>, ValueRange, Data2),
    nebula2_riak:update(Pid, Oid, Data3).

%% ====================================================================
%% Internal functions
%% ====================================================================

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).
-endif.