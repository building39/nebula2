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
            get_domain/2,
            new_domain/2,
            update_domain/3
        ]).

%% @doc Get a CDMI domain
-spec nebula2_domains:get_domain(pid(), object_oid()) -> {ok, json_value()}.
get_domain(Pid, Oid) ->
    {ok, Data} = nebula2_riak:get(Pid, Oid),
    jsx:decode(list_to_binary(Data), [return_maps]).

%% @doc Create a new CDMI domain
-spec nebula2_domains:new_domain(Req, State) -> {boolean(), Req, State}
        when Req::cowboy_req:req().
new_domain(Req, State) ->
    DomainName = "fake domain",
    ObjectType = ?CONTENT_TYPE_CDMI_DOMAIN,
    nebula2_utils:create_object(Req, State, ObjectType, DomainName).

%% @doc Update a CDMI domain
-spec nebula2_domains:update_domain(pid(), object_oid(), map()) -> {ok, json_value()}.
update_domain(Pid, ObjectId, Data) ->
    lager:debug("nebula2_domains:update_domain: Pid: ~p ObjectId: ~p Data: ~p", [Pid, ObjectId, Data]),
    NewData = Data,
    {ok, NewData}.

%% ====================================================================
%% Internal functions
%% ====================================================================


%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).
-endif.