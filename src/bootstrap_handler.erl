%% @author mmartin
%% @doc 
%% Bootstrap the server
%% @end

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
    {ok, B, Req2} = cowboy_req:body(Req),
    % lager:debug("bootstrap: from_cdmi_object: Body: ~p", [Body]),
    Body = jsx:decode(B, [return_maps]),
    Data = nebula2_db:marshall(Body),
    Oid = nebula2_utils:make_key(),
    % lager:debug("bootstrap: from_cdmi_object: Oid: ~p", [Oid]),
    Data2 = nebula2_utils:put_value(<<"objectID">>, Oid, Data),
    ParentID = nebula2_utils:get_value(<<"parentID">>, Data2),
    % lager:debug("bootstrap: parentID: ~p", [ParentID]),
    {Path, Req3} = cowboy_req:path(Req2),
    % lager:debug("bootstrap: from_cdmi_object: Path: ~p", [Path]),
    ObjectType = nebula2_utils:get_value(<<"objectType">>, Data2),
    % lager:debug("Data2: ~p", [Data2]),
    {ok, Oid} = nebula2_db:create(Pid, Oid, Data2),
    nebula2_utils:update_parent(ParentID,
                                binary_to_list(Path),
                                ObjectType,
                                Pid),
    pooler:return_member(riak_pool, Pid),
    ResponseBody = nebula2_db:unmarshall(Data2),
    Req4 = cowboy_req:set_resp_body(jsx:encode(maps:to_list(ResponseBody)), Req3),
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
    Path = binary_to_list(nebula2_utils:get_value(<<"path">>, EnvMap)),
    Response = case nebula2_db:search(Path, State) of
                   {error, _Status} ->
                       false;
                   _Other ->
                       true
               end,
    NewEnvMap = nebula2_utils:put_value(<<"exists">>, Response, EnvMap),
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


