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

-export([register_connection/1, unregister_connection/1]).

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
    mnesia:write(?TABLE, Conn, write).

unregister_connection(ConnName) ->
    mnesia:delete({?TABLE, ConnName}).

