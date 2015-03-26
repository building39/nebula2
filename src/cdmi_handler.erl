-module(cdmi_handler).

-export([init/3]).
-export([content_types_provided/2]).
-export([get_html/2]).

init(_, _Req, _Opts) ->
    lager:debug("initing..."),
    {upgrade, protocol, cowboy_rest}.

content_types_provided(Req, State) ->
    {[{{<<"text">>, <<"html">>, '*'}, get_html}], Req, State}.

get_html(Req, State) ->
    lager:info("get_html..."),
    {<<"<html><body>This is REST!</body></html>">>, Req, State}.
