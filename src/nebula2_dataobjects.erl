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
            new_dataobject/3,
            update_dataobject/4
        ]).

%% @doc Get a CDMI dataobject
-spec nebula2_dataobjects:get_dataobject(pid(), object_oid()) -> {ok, json_value()}.
get_dataobject(Pid, Oid) ->
    lager:debug("Entry"),
    {ok, Data} = nebula2_db:read(Pid, Oid),
    Data.

%% @doc Create a new CDMI dataobject
-spec nebula2_dataobjects:new_dataobject(cowboy_req:req(), cdmi_state(), map()) -> {boolean(), cowboy_req:req(), cdmi_state()}.
new_dataobject(Req, State, Body) ->
    lager:debug("Entry"),
    ObjectType = ?CONTENT_TYPE_CDMI_DATAOBJECT,
    Response = case nebula2_utils:create_object(Req, State, ObjectType, Body) of
                   {true, Req2, Data} ->
                       {true, cowboy_req:set_resp_body(jsx:encode(maps:to_list(Data)), Req2), State};
                   false ->
                       {false, Req, State}
               end,
    Response.

%% @doc Update a CDMI dataobject
-spec nebula2_dataobjects:update_dataobject(cowboy_req:req(), cdmi_state(), object_oid(), map()) -> {ok, json_value()}.
update_dataobject(Req, State, Oid, NewData) ->
    lager:debug("Entry"),
    {Pid, _EnvMap} = State,
    nebula2_utils:check_base64(NewData),
    {ok, OldData} = nebula2_db:read(Pid, Oid),
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
    Response = case nebula2_db:update(Pid, Oid, Data3) of
                   ok ->
                       {true, Req, State};
                   _  ->
                       {false, Req, State}
               end,
    Response.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).
-endif.
