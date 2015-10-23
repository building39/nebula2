%% @author mmartin
%% @doc @todo Add description to bootstrap_handler.


-module(bootstrap_handler).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("nebula.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/3,
         allowed_methods/2,
         content_types_accepted/2,
         content_types_provided/2,
         from_cdmi_capability/2,
         from_cdmi_container/2,
         from_cdmi_domain/2,
         from_cdmi_object/2,
         is_conflict/2,
         known_methods/2,
         resource_exists/2,
         rest_init/2,
         service_available/2
        ]).

init(_, _Req, _Opts) ->
    lager:debug("Entry"),
    % lager:debug("bootstrap initing..."),
    {upgrade, protocol, cowboy_rest}.

%% Bootstrap can only PUT new objects.
allowed_methods(Req, State) ->
    lager:debug("Entry"),
    {[<<"PUT">>], Req, State}.

content_types_accepted(Req, State) ->
    lager:debug("Entry"),
    cdmi_handler:content_types_accepted(Req, State).

content_types_provided(Req, State) ->
    lager:debug("Entry"),
    cdmi_handler:content_types_provided(Req, State).

from_cdmi_capability(Req, State) ->
    lager:debug("Entry"),
    from_cdmi_object(Req, State).

from_cdmi_container(Req, State) ->
    lager:debug("Entry"),
    from_cdmi_object(Req, State).

from_cdmi_domain(Req, State) ->
    lager:debug("Entry"),
    from_cdmi_object(Req, State).

from_cdmi_object(Req, State) ->
    lager:debug("Entry"),
    {Pid, _EnvMap} = State,
    {ok, Body, Req2} = cowboy_req:body(Req),
    % lager:debug("bootstrap: from_cdmi_object: Body: ~p", [Body]),
    Data = jsx:decode(Body, [return_maps]),
    Oid = nebula2_utils:make_key(),
    % lager:debug("bootstrap: from_cdmi_object: Oid: ~p", [Oid]),
    Data2 = maps:put(<<"objectID">>, list_to_binary(Oid), Data),
    ParentID = maps:get(<<"parentID">>, Data2, <<"">>),
    % lager:debug("bootstrap: parentID: ~p", [ParentID]),
    {Path, Req3} = cowboy_req:path(Req2),
    lager:debug("bootstrap: from_cdmi_object: Path: ~p", [Path]),
    ObjectType = maps:get(<<"objectType">>, Data2),
    % lager:debug("Data2: ~p", [Data2]),
    {ok, Oid} = nebula2_db:create(Pid, Oid, Data2),
    nebula2_utils:update_parent(ParentID,
                                binary_to_list(Path),
                                binary_to_list(ObjectType),
                                Pid),
    pooler:return_member(riak_pool, Pid),
    L = maps:to_list(Data2),
    B = jsx:encode(L),
    Req4 = cowboy_req:set_resp_body(B, Req3),
    {true, Req4, State}.

is_conflict(Req, State) ->
    lager:debug("Entry"),
    cdmi_handler:is_conflict(Req, State).

%% Bootstrap can only PUT new objects.
known_methods(Req, State) ->
    lager:debug("Entry"),
    {[<<"PUT">>], Req, State}.

resource_exists(Req, State) ->
    lager:debug("Entry"),
    % lager:debug("bootstrap: resource_exists:"),
    {Pid, EnvMap} = State,
    Path = binary_to_list(maps:get(<<"path">>, EnvMap)),
    Response = case nebula2_db:search(Path, State) of
                   {error, _Status} ->
                       false;
                   _Other ->
                       true
               end,
    NewEnvMap = maps:put(<<"exists">>, Response, EnvMap),
    {Response, Req, {Pid, NewEnvMap}}.

rest_init(Req, State) ->
    lager:debug("Entry"),
    cdmi_handler:rest_init(Req, State).

%% Call service_available in the cdmi handler
service_available(Req, State) ->
    lager:debug("Entry"),
    cdmi_handler:service_available(Req, State).

%% ====================================================================
%% Internal functions
%% ====================================================================


