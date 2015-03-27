%% @author mmartin
%% @doc @todo Add description to nebula_riak.


-module(nebula2_riak).
-compile([{parse_transform, lager_transform}]).

-include_lib("riakc/include/riakc.hrl").
-include("nebula.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([get/2,
         ping/1]).

%% @doc Get a value from riak by bucket type, bucket and key.
-spec nebula_riak:get(object_oid()) -> {ok, json_value()}.
get(Conn, Oid) ->
    lager:info("Type: ~p Bucket: ~p Oid: ~p", [?BUCKET_TYPE, ?BUCKET_NAME, Oid]),
    Response = case riakc_pb_socket:get(Conn,
                                        {list_to_binary(?BUCKET_TYPE),
                                         list_to_binary(?BUCKET_NAME)},
                                         list_to_binary(Oid)) of
                    {ok, Object} ->
                        Contents = binary_to_list(riakc_obj:get_value(Object)),
                        {ok, Contents};
                    {error, Term} ->
                        {error, Term}
    end,
    lager:info("Get response is ~p", [Response]),
    Response.

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
