%% @author mmartin
%% @doc Handle CDMI domain objects.

-module(nebula2_domains).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("nebula.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([
            delete_domain/2,
            new_domain/2,
            update_domain/3
        ]).

%% @doc Delete a CDMI domain
-spec nebula2_domains:delete_domain(cowboy_req:req(), {pid(), map()}) -> ok | {error, term()}.
delete_domain(Req, State) ->
    % lager:debug("Entry nebula2_domains:delete_domain"),
    {Pid, EnvMap} = State,
    Data = maps:get(<<"object_map">>, EnvMap),
    Oid = maps:get(<<"objectID">>, Data),
    handle_delete(nebula2_riak:delete(Pid, Oid), Req, State).

%% @doc Create a new CDMI domain
-spec nebula2_domains:new_domain(Req, State) -> {boolean(), Req, State}
        when Req::cowboy_req:req().
new_domain(Req, State) ->
    % lager:debug("Entry nebula2_domains:new_domain"),
    {_Pid, EnvMap} = State,
    DomainName = maps:get(<<"parentURI">>, EnvMap) ++ maps:get(<<"objectName">>, EnvMap),
    ObjectType = ?CONTENT_TYPE_CDMI_DOMAIN,
    nebula2_utils:create_object(Req, State, ObjectType, DomainName).

%% @doc Update a CDMI domain
-spec nebula2_domains:update_domain(pid(), object_oid(), map()) -> {ok, json_value()}.
update_domain(_Pid, _ObjectId, Data) ->
    % lager:debug("Entry nebula2_domains:update_domain"),
    % lager:debug("nebula2_domains:update_domain: Pid: ~p ObjectId: ~p Data: ~p", [Pid, ObjectId, Data]),
    NewData = Data,
    {ok, NewData}.

%% ====================================================================
%% Internal functions
%% ====================================================================
handle_delete(ok, Req, State) ->
    % lager:debug("Entry nebula2_domains:handle_delete - delete succeeded"),
    {true, Req, State};
handle_delete({error, _Error}, Req, State) ->
    % lager:debug("Entry nebula2_domains:handle_delete - delete failed reason: ~p", [Error]),
    {false, Req, State}.

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).
-endif.