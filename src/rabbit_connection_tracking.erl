%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_connection_tracking).

%% Abstracts away how tracked connection records are stored
%% and queried.
%%
%% See also:
%%
%%  * rabbit_connection_tracking_handler
%%  * rabbit_reader
%%  * rabbit_event

-export([register_connection/1, unregister_connection/1,
         tracked_connection_from_connection_created/1,
         is_over_connection_limit/1, count_connections_in/1]).

-ifdef(use_specs).

-spec(register_connection/1   :: (rabbit_types:tracked_connection()) -> ok).
-spec(unregister_connection/1 :: (rabbit_types:connection_name()) -> ok).

-endif.

-include_lib("rabbit.hrl").

-define(TABLE,  rabbit_tracked_connection).
-define(SERVER, ?MODULE).

%%
%% API
%%

register_connection(Conn) ->
    rabbit_misc:execute_mnesia_transaction(fun() ->
        mnesia:write(?TABLE, Conn, write)
    end).

unregister_connection(ConnId = {_Node, _Name}) ->
    rabbit_misc:execute_mnesia_transaction(fun() ->
        mnesia:delete({?TABLE, ConnId})
    end).

is_over_connection_limit(VirtualHost) ->
    ConnectionCount = count_connections_in(VirtualHost),
    case rabbit_vhost_limit:connection_limit(VirtualHost) of
        undefined   -> false;
        {ok, Limit} -> case ConnectionCount > Limit of
                           false -> false;
                           true  -> {true, Limit}
                       end
    end.

count_connections_in(VirtualHost) ->
    ets:select_count(?TABLE, [{#tracked_connection{vhost = '$1', _ = '_'},
                              [{'=:=','$1', VirtualHost}],
                              [true]}]).

%% Returns a #tracked_connection from connection_created
%% event details.
%%
%% @see rabbit_connection_tracking_handler.
tracked_connection_from_connection_created(EventDetails) ->
    %% Example event:
    %%
    %% [{type,network},
    %%  {pid,<0.329.0>},
    %%  {name,<<"127.0.0.1:60998 -> 127.0.0.1:5672">>},
    %%  {port,5672},
    %%  {peer_port,60998},
    %%  {host,{0,0,0,0,0,65535,32512,1}},
    %%  {peer_host,{0,0,0,0,0,65535,32512,1}},
    %%  {ssl,false},
    %%  {peer_cert_subject,''},
    %%  {peer_cert_issuer,''},
    %%  {peer_cert_validity,''},
    %%  {auth_mechanism,<<"PLAIN">>},
    %%  {ssl_protocol,''},
    %%  {ssl_key_exchange,''},
    %%  {ssl_cipher,''},
    %%  {ssl_hash,''},
    %%  {protocol,{0,9,1}},
    %%  {user,<<"guest">>},
    %%  {vhost,<<"/">>},
    %%  {timeout,14},
    %%  {frame_max,131072},
    %%  {channel_max,65535},
    %%  {client_properties,
    %%      [{<<"capabilities">>,table,
    %%        [{<<"publisher_confirms">>,bool,true},
    %%         {<<"consumer_cancel_notify">>,bool,true},
    %%         {<<"exchange_exchange_bindings">>,bool,true},
    %%         {<<"basic.nack">>,bool,true},
    %%         {<<"connection.blocked">>,bool,true},
    %%         {<<"authentication_failure_close">>,bool,true}]},
    %%       {<<"product">>,longstr,<<"Bunny">>},
    %%       {<<"platform">>,longstr,
    %%        <<"ruby 2.3.0p0 (2015-12-25 revision 53290) [x86_64-darwin15]">>},
    %%       {<<"version">>,longstr,<<"2.3.0.pre">>},
    %%       {<<"information">>,longstr,
    %%        <<"http://rubybunny.info">>}]},
    %%  {connected_at,1453214290847}]
    Name = proplists:get_value(name, EventDetails),
    Node = proplists:get_value(node, EventDetails),
    #tracked_connection{id           = {Node, Name},
                        name         = Name,
                        node         = Node,
                        vhost        = proplists:get_value(vhost, EventDetails),
                        username     = proplists:get_value(user, EventDetails),
                        connected_at = proplists:get_value(connected_at, EventDetails),
                        pid          = proplists:get_value(pid, EventDetails),
                        peer_host    = proplists:get_value(peer_host, EventDetails),
                        peer_port    = proplists:get_value(peer_port, EventDetails)}.
