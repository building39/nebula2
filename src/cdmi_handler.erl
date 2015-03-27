-module(cdmi_handler).
-compile([{parse_transform, lager_transform}]).

-export([init/3]).
-export([content_types_provided/2]).
-export([get_html/2, rest_init/2, service_available/2]).

init(_, _Req, _Opts) ->
    lager:debug("initing..."),
    {upgrade, protocol, cowboy_rest}.

rest_init(Req, _State) ->
    PoolMember = pooler:take_member(riak_pool),
    lager:debug("PoolMember: ~p", [PoolMember]),
    {ok, Req, PoolMember}.

content_types_provided(Req, State) ->
    {[{{<<"text">>, <<"html">>, '*'}, get_html}], Req, State}.

get_html(Req, State) ->
    lager:debug("get_html...~p", [State]),
    pooler:return_member(riak_pool, State),
    {<<"<html><body>This is REST!</body></html>">>, Req, State}.

%% if pooler says no members, kick back a 503. I
%% do this here because a 503 seems to me the most
%% appropriate response if database connections are
%% <b>currently</b> unavailable.
service_available(Req, error_no_members) ->
    {false, Req, undefined};
service_available(Req, Conn) ->
    Available = case nebula2_riak:ping(Conn) of
        true ->
             lager:debug("Service available..."),
             true;
        _ -> lager:debug("Service unavailable..."),
             false
    end,
    {Available, Req, Conn}.