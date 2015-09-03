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
%% TODO: Enforce CDMI domain deletion rules - i.e. move populated domains to metadata: cdmi_domain_delete_reassign
%%       if the domain contains objects and then delete the domain, or fail with 400 if the domain contains objects
%%       and metadata: cdmi_domain_delete_reassign is missing or points to a non-existent domain.
-spec nebula2_domains:delete_domain(cowboy_req:req(), cdmi_state()) -> ok | {error, term()}.
delete_domain(Req, State) ->
    ?LOG_ENTRY,
    {Pid, EnvMap} = State,
    Path = maps:get(<<"path">>, EnvMap),
    Data = nebula2_riak:search(Path, State),
    Oid = maps:get(<<"objectID">>, Data),
    Metadata = maps:get(<<"metadata">>, Data),
    ReassignTo = maps:get(<<"cdmi_domain_delete_reassign">>, Metadata, nil),
    case maps:get(<<"cdmi_domain_delete_reassign">>, Metadata, nil) of
        nil ->
            case maps:get(<<"children">>, Data, []) of
                [] ->
                    handle_delete(nebula2_riak:delete(Pid, Oid), Req, State);
                _ ->
                    {error, 400}
            end;
        ReassignTo ->
            {error, 501}
    end.

%% @doc Create a new CDMI domain
-spec nebula2_domains:new_domain(cowboy_req:req(), cdmi_state()) -> {boolean(), cowboy_req:req(), cdmi_state()}.
new_domain(Req, State) ->
    ?LOG_ENTRY,
    {_Pid, EnvMap} = State,
    DomainName = maps:get(<<"parentURI">>, EnvMap) ++ maps:get(<<"objectName">>, EnvMap),
    ObjectType = ?CONTENT_TYPE_CDMI_DOMAIN,
    {ok, B, Req2} = cowboy_req:body(Req),
    Body = jsx:decode(B, [return_maps]),
    Response = case nebula2_utils:create_object(Body, State, ObjectType, DomainName) of
                   {true, Data} ->
                       {true, cowboy_req:set_resp_body(jsx:encode(maps:to_list(Data)), Req2), State};
                   false ->
                       {false, Req2, State}
               end,
    Response.
    

%% @doc Update a CDMI domain
-spec nebula2_domains:update_domain(pid(), object_oid(), map()) -> {ok, json_value()}.
update_domain(_Pid, _ObjectId, Data) ->
    ?LOG_ENTRY,
    NewData = Data,
    {ok, NewData}.

%% ====================================================================
%% Internal functions
%% ====================================================================
handle_delete(ok, Req, State) ->
    ?LOG_ENTRY,
    {true, Req, State};
handle_delete({error, _Error}, Req, State) ->
    ?LOG_ENTRY,
    {false, Req, State}.

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).
-endif.