%% @author mmartin
%% @doc @todo Add description to nebula_riak.


-module(nebula2_riak).
-compile([{parse_transform, lager_transform}]).

-include_lib("riakc/include/riakc.hrl").
-include("nebula.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([ping/1]).

%% @doc Ping the riak cluster.
-spec nebula_riak:ping(pid()) -> boolean().
ping(Pid) ->
    case riakc_pb_socket:ping(Pid) of
        pong -> lager:debug("Connected to Riak..."),
                true;
        R -> lager:debug("Can't ping Riak: ~p", [R]),
             false
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================
