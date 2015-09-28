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
    lager:debug("Entry"),
    {ok, Data} = nebula2_db:read(Pid, Oid),
    Data.

%% @doc Create a new CDMI dataobject
-spec nebula2_dataobjects:new_dataobject(cowboy_req:req(), cdmi_state()) -> {boolean(), cowboy_req:req(), cdmi_state()}.
new_dataobject(Req, State) ->
    lager:debug("Entry"),
    ObjectType = ?CONTENT_TYPE_CDMI_DATAOBJECT,
    DomainName = "fake domain",
    Response = case nebula2_utils:create_object(Req, State, ObjectType, DomainName) of
                   {true, Req2, Data} ->
                       {true, cowboy_req:set_resp_body(jsx:encode(maps:to_list(Data)), Req2), State};
                   false ->
                       {false, Req, State}
               end,
    Response.

%% @doc Update a CDMI dataobject
-spec nebula2_dataobjects:update_dataobject(pid(), object_oid(), map()) -> {ok, json_value()}.
update_dataobject(Pid, Oid, NewData) ->
    lager:debug("Entry"),
    {ok, OldData} = nebula2_db:read(Pid, Oid),
    OldMetaData = maps:get(<<"metadata">>, OldData, maps:new()),
    NewMetaData = maps:get(<<"metadata">>, NewData, maps:new()),
    MetaData = maps:merge(OldMetaData, NewMetaData),
    Data = maps:merge(OldData, NewData),
    Data2 = maps:put(<<"metadata">>, MetaData, Data),
    Value = binary_to_list(jsx:encode(maps:get(<<"value">>, Data2))),
    Length = length(Value) - 1,
    ValueRange = list_to_binary(lists:flatten(io_lib:format("0-~p", [Length]))),
    Data3 = maps:put(<<"valuerange">>, ValueRange, Data2),
    nebula2_db:update(Pid, Oid, Data3).

%% ====================================================================
%% Internal functions
%% ====================================================================

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).
-endif.
