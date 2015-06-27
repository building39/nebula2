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
    lager:debug("bootstrap initing..."),
    {upgrade, protocol, cowboy_rest}.

%% Bootstrap can only PUT new objects.
allowed_methods(Req, State) ->
    {[<<"PUT">>], Req, State}.

content_types_accepted(Req, State) ->
    cdmi_handler:content_types_accepted(Req, State).

content_types_provided(Req, State) ->
    cdmi_handler:content_types_provided(Req, State).

from_cdmi_capability(Req, State) ->
    from_cdmi_object(Req, State).

from_cdmi_container(Req, State) ->
    from_cdmi_object(Req, State).

from_cdmi_domain(Req, State) ->
    from_cdmi_object(Req, State).

from_cdmi_object(Req, State) ->
    {Pid, EnvMap} = State,
    Body = maps:get(<<"body">>, EnvMap),
    lager:debug("bootstrap: from_cdmi_object: Body: ~p", [Body]),
    Oid = nebula2_utils:make_key(),
    lager:debug("bootstrap: from_cdmi_object: Oid: ~p", [Oid]),
    Data2 = maps:put(<<"objectID">>, list_to_binary(Oid), Body),
    ParentID = maps:get(<<"parentID">>, Data2, <<"">>),
    {Path, Req2} = cowboy_req:path(Req),
    lager:debug("bootstrap: from_cdmi_object: Path: ~p", [Path]),
    ObjectType = maps:get(<<"objectType">>, Data2),
    ObjectName = maps:get(<<"objectName">>, Data2),
    {ok, Oid} = nebula2_riak:put(Pid, ObjectName, Oid, Data2),
    nebula2_utils:update_parent(ParentID,
                                binary_to_list(Path),
                                binary_to_list(ObjectType),
                                Pid),
    pooler:return_member(riak_pool, Pid),
    L = maps:to_list(Data2),
    B = jsx:encode(L),
    Req3 = cowboy_req:set_resp_body(B, Req2),
    {true, Req3, State}.

is_conflict(Req, State) ->
    cdmi_handler:is_conflict(Req, State).

%% Bootstrap can only PUT new objects.
known_methods(Req, State) ->
    {[<<"PUT">>], Req, State}.

resource_exists(Req, State) ->
    lager:debug("bootstrap: resource_exists:"),
    {Pid, EnvMap} = State,
    Path = maps:get(<<"path">>, EnvMap),
    Response = case nebula2_riak:search(Path, State) of
                   {error, _Status} ->
                       false;
                   _Other ->
                       true
               end,
    NewEnvMap = maps:put(<<"exists">>, Response, EnvMap),
    {Response, Req, {Pid, NewEnvMap}}.

rest_init(Req, State) ->
    cdmi_handler:rest_init(Req, State).

%% Call service_available in the cdmi handler
service_available(Req, State) ->
    cdmi_handler:service_available(Req, State).

%% ====================================================================
%% Internal functions
%% ====================================================================


