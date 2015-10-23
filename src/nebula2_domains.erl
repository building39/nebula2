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
    %% lager:debug("Entry"),
    {Pid, EnvMap} = State,
    Path = binary_to_list(maps:get(<<"path">>, EnvMap)),
    Data = nebula2_db:search(Path, State),
    Oid = maps:get(<<"objectID">>, Data),
    Metadata = maps:get(<<"metadata">>, Data),
    ReassignTo = maps:get(<<"cdmi_domain_delete_reassign">>, Metadata, nil),
    case maps:get(<<"cdmi_domain_delete_reassign">>, Metadata, nil) of
        nil ->
            case maps:get(<<"children">>, Data, []) of
                [] ->
                    handle_delete(nebula2_db:delete(Pid, Oid), Req, State);
                _ ->
                    {error, 400}
            end;
        ReassignTo ->
            {error, 501}
    end.

%% @doc Create a new CDMI domain
-spec nebula2_domains:new_domain(cowboy_req:req(), cdmi_state()) -> {boolean(), cowboy_req:req(), cdmi_state()}.
new_domain(Req, State) ->
    %% lager:debug("Entry"),
    {_Pid, EnvMap} = State,
    DomainName = binary_to_list(maps:get(<<"parentURI">>, EnvMap)) ++ binary_to_list(maps:get(<<"objectName">>, EnvMap)),
    lager:debug("Domain name: ~p", [DomainName]),
    ObjectType = ?CONTENT_TYPE_CDMI_DOMAIN,
    {ok, ReqBody, Req2} = cowboy_req:body(Req),
    Body2 = try jsx:decode(ReqBody, [return_maps]) of
                NewBody -> NewBody
            catch
                error:badarg ->
                    throw(badjson)
            end,
    Response = case nebula2_utils:create_object(Req2, State, ObjectType, DomainName, Body2) of
                   {true, Req3, Data} ->
                       {true, cowboy_req:set_resp_body(jsx:encode(maps:to_list(Data)), Req3), State};
                   false ->
                       {false, Req2, State}
               end,
    Response.
    

%% @doc Update a CDMI domain
-spec nebula2_domains:update_domain(pid(), object_oid(), map()) -> {ok, json_value()}.
update_domain(_Pid, _ObjectId, Data) ->
    %% lager:debug("Entry"),
    NewData = Data,
    {ok, NewData}.

%% ====================================================================
%% Internal functions
%% ====================================================================
handle_delete(ok, Req, State) ->
    %% lager:debug("Entry"),
    {true, Req, State};
handle_delete({error, _Error}, Req, State) ->
    %% lager:debug("Entry"),
    {false, Req, State}.

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).
-endif.
