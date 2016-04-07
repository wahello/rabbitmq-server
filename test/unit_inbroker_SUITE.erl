%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(unit_inbroker_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-compile(export_all).

-define(PERSISTENT_MSG_STORE, msg_store_persistent).
-define(TRANSIENT_MSG_STORE,  msg_store_transient).

-define(TIMEOUT_LIST_OPS_PASS, 1000).
-define(TIMEOUT, 30000).

-define(VARIABLE_QUEUE_TESTCASES, [
    variable_queue_dynamic_duration_change,
    variable_queue_partial_segments_delta_thing,
    variable_queue_all_the_bits_not_covered_elsewhere_A,
    variable_queue_all_the_bits_not_covered_elsewhere_B,
    variable_queue_drop,
    variable_queue_fold_msg_on_disk,
    variable_queue_dropfetchwhile,
    variable_queue_dropwhile_varying_ram_duration,
    variable_queue_fetchwhile_varying_ram_duration,
    variable_queue_ack_limiting,
    variable_queue_purge,
    variable_queue_requeue,
    variable_queue_requeue_ram_beta,
    variable_queue_fold,
    variable_queue_batch_publish,
    variable_queue_batch_publish_delivered
  ]).

-define(BACKING_QUEUE_TESTCASES, [
    bq_queue_index,
    bq_queue_index_props,
    {variable_queue_default, [], ?VARIABLE_QUEUE_TESTCASES},
    {variable_queue_lazy, [], ?VARIABLE_QUEUE_TESTCASES ++
                              [variable_queue_mode_change]},
    bq_variable_queue_delete_msg_store_files_callback,
    bq_queue_recover
  ]).

all() ->
    [
      {group, parallel_tests},
      {group, backing_queue}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
          {credit_flow, [parallel], [
              credit_flow_settings
            ]},
          file_handle_cache,
          {password_hashing, [parallel], [
              password_hashing,
              change_password
            ]},
          log_management,
          log_management_during_startup,
          {rabbitmqctl, [parallel], [
              list_operations_timeout_pass
            ]},
          topic_matching
        ]},
      {backing_queue, [], [
          msg_store,
          {backing_queue_embed_limit_0, [], ?BACKING_QUEUE_TESTCASES},
          {backing_queue_embed_limit_1024, [], ?BACKING_QUEUE_TESTCASES}
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:run_setup_steps(?MODULE, Config),
    rabbit_ct_helpers:run_steps(Config1, [
        fun setup_file_handle_cache/1
      ]).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

setup_file_handle_cache(Config) ->
    ok = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, setup_file_handle_cache1, []),
    Config.

setup_file_handle_cache1() ->
    %% FIXME: Why are we doing this?
    application:set_env(rabbit, file_handles_high_watermark, 10),
    ok = file_handle_cache:set_limit(10),
    ok.

init_per_group(backing_queue, Config) ->
    Node = ?config(rmq_nodename, Config),
    Module = rabbit_ct_broker_helpers:run_on_broker(Node,
      application, get_env, [rabbit, backing_queue_module]),
    case Module of
        {ok, rabbit_priority_queue} ->
            rabbit_ct_broker_helpers:run_on_broker(Node,
              ?MODULE, setup_backing_queue_test_group, [Config]);
        _ ->
            {skip, rabbit_misc:format(
               "Backing queue module not supported by this test group: ~p~n",
               [Module])}
    end;
init_per_group(backing_queue_embed_limit_0, Config) ->
    ok = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      application, set_env, [rabbit, queue_index_embed_msgs_below, 0]),
    Config;
init_per_group(backing_queue_embed_limit_1024, Config) ->
    ok = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      application, set_env, [rabbit, queue_index_embed_msgs_below, 1024]),
    Config;
init_per_group(variable_queue_default, Config) ->
    rabbit_ct_helpers:set_config(Config, {variable_queue_type, default});
init_per_group(variable_queue_lazy, Config) ->
    rabbit_ct_helpers:set_config(Config, {variable_queue_type, lazy});
init_per_group(_, Config) ->
    Config.

end_per_group(backing_queue, Config) ->
    rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, teardown_backing_queue_test_group, [Config]);
end_per_group(Group, Config)
when   Group =:= backing_queue_embed_limit_0
orelse Group =:= backing_queue_embed_limit_1024 ->
    ok = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      application, set_env, [rabbit, queue_index_embed_msgs_below,
        ?config(rmq_queue_index_embed_msgs_below, Config)]),
    Config;
end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Message store.
%% -------------------------------------------------------------------

msg_store(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, msg_store1, [Config]).

msg_store1(_Config) ->
    restart_msg_store_empty(),
    MsgIds = [msg_id_bin(M) || M <- lists:seq(1,100)],
    {MsgIds1stHalf, MsgIds2ndHalf} = lists:split(length(MsgIds) div 2, MsgIds),
    Ref = rabbit_guid:gen(),
    {Cap, MSCState} = msg_store_client_init_capture(
                        ?PERSISTENT_MSG_STORE, Ref),
    Ref2 = rabbit_guid:gen(),
    {Cap2, MSC2State} = msg_store_client_init_capture(
                          ?PERSISTENT_MSG_STORE, Ref2),
    %% check we don't contain any of the msgs we're about to publish
    false = msg_store_contains(false, MsgIds, MSCState),
    %% test confirm logic
    passed = test_msg_store_confirms([hd(MsgIds)], Cap, MSCState),
    %% check we don't contain any of the msgs we're about to publish
    false = msg_store_contains(false, MsgIds, MSCState),
    %% publish the first half
    ok = msg_store_write(MsgIds1stHalf, MSCState),
    %% sync on the first half
    ok = on_disk_await(Cap, MsgIds1stHalf),
    %% publish the second half
    ok = msg_store_write(MsgIds2ndHalf, MSCState),
    %% check they're all in there
    true = msg_store_contains(true, MsgIds, MSCState),
    %% publish the latter half twice so we hit the caching and ref
    %% count code. We need to do this through a 2nd client since a
    %% single client is not supposed to write the same message more
    %% than once without first removing it.
    ok = msg_store_write(MsgIds2ndHalf, MSC2State),
    %% check they're still all in there
    true = msg_store_contains(true, MsgIds, MSCState),
    %% sync on the 2nd half
    ok = on_disk_await(Cap2, MsgIds2ndHalf),
    %% cleanup
    ok = on_disk_stop(Cap2),
    ok = rabbit_msg_store:client_delete_and_terminate(MSC2State),
    ok = on_disk_stop(Cap),
    %% read them all
    MSCState1 = msg_store_read(MsgIds, MSCState),
    %% read them all again - this will hit the cache, not disk
    MSCState2 = msg_store_read(MsgIds, MSCState1),
    %% remove them all
    ok = msg_store_remove(MsgIds, MSCState2),
    %% check first half doesn't exist
    false = msg_store_contains(false, MsgIds1stHalf, MSCState2),
    %% check second half does exist
    true = msg_store_contains(true, MsgIds2ndHalf, MSCState2),
    %% read the second half again
    MSCState3 = msg_store_read(MsgIds2ndHalf, MSCState2),
    %% read the second half again, just for fun (aka code coverage)
    MSCState4 = msg_store_read(MsgIds2ndHalf, MSCState3),
    ok = rabbit_msg_store:client_terminate(MSCState4),
    %% stop and restart, preserving every other msg in 2nd half
    ok = rabbit_variable_queue:stop_msg_store(),
    ok = rabbit_variable_queue:start_msg_store(
           [], {fun ([]) -> finished;
                    ([MsgId|MsgIdsTail])
                      when length(MsgIdsTail) rem 2 == 0 ->
                        {MsgId, 1, MsgIdsTail};
                    ([MsgId|MsgIdsTail]) ->
                        {MsgId, 0, MsgIdsTail}
                end, MsgIds2ndHalf}),
    MSCState5 = msg_store_client_init(?PERSISTENT_MSG_STORE, Ref),
    %% check we have the right msgs left
    lists:foldl(
      fun (MsgId, Bool) ->
              not(Bool = rabbit_msg_store:contains(MsgId, MSCState5))
      end, false, MsgIds2ndHalf),
    ok = rabbit_msg_store:client_terminate(MSCState5),
    %% restart empty
    restart_msg_store_empty(),
    MSCState6 = msg_store_client_init(?PERSISTENT_MSG_STORE, Ref),
    %% check we don't contain any of the msgs
    false = msg_store_contains(false, MsgIds, MSCState6),
    %% publish the first half again
    ok = msg_store_write(MsgIds1stHalf, MSCState6),
    %% this should force some sort of sync internally otherwise misread
    ok = rabbit_msg_store:client_terminate(
           msg_store_read(MsgIds1stHalf, MSCState6)),
    MSCState7 = msg_store_client_init(?PERSISTENT_MSG_STORE, Ref),
    ok = msg_store_remove(MsgIds1stHalf, MSCState7),
    ok = rabbit_msg_store:client_terminate(MSCState7),
    %% restart empty
    restart_msg_store_empty(), %% now safe to reuse msg_ids
    %% push a lot of msgs in... at least 100 files worth
    {ok, FileSize} = application:get_env(rabbit, msg_store_file_size_limit),
    PayloadSizeBits = 65536,
    BigCount = trunc(100 * FileSize / (PayloadSizeBits div 8)),
    MsgIdsBig = [msg_id_bin(X) || X <- lists:seq(1, BigCount)],
    Payload = << 0:PayloadSizeBits >>,
    ok = with_msg_store_client(
           ?PERSISTENT_MSG_STORE, Ref,
           fun (MSCStateM) ->
                   [ok = rabbit_msg_store:write(MsgId, Payload, MSCStateM) ||
                       MsgId <- MsgIdsBig],
                   MSCStateM
           end),
    %% now read them to ensure we hit the fast client-side reading
    ok = foreach_with_msg_store_client(
           ?PERSISTENT_MSG_STORE, Ref,
           fun (MsgId, MSCStateM) ->
                   {{ok, Payload}, MSCStateN} = rabbit_msg_store:read(
                                                  MsgId, MSCStateM),
                   MSCStateN
           end, MsgIdsBig),
    %% .., then 3s by 1...
    ok = msg_store_remove(?PERSISTENT_MSG_STORE, Ref,
                          [msg_id_bin(X) || X <- lists:seq(BigCount, 1, -3)]),
    %% .., then remove 3s by 2, from the young end first. This hits
    %% GC (under 50% good data left, but no empty files. Must GC).
    ok = msg_store_remove(?PERSISTENT_MSG_STORE, Ref,
                          [msg_id_bin(X) || X <- lists:seq(BigCount-1, 1, -3)]),
    %% .., then remove 3s by 3, from the young end first. This hits
    %% GC...
    ok = msg_store_remove(?PERSISTENT_MSG_STORE, Ref,
                          [msg_id_bin(X) || X <- lists:seq(BigCount-2, 1, -3)]),
    %% ensure empty
    ok = with_msg_store_client(
           ?PERSISTENT_MSG_STORE, Ref,
           fun (MSCStateM) ->
                   false = msg_store_contains(false, MsgIdsBig, MSCStateM),
                   MSCStateM
           end),
    %%
    passed = test_msg_store_client_delete_and_terminate(),
    %% restart empty
    restart_msg_store_empty(),
    passed.

restart_msg_store_empty() ->
    ok = rabbit_variable_queue:stop_msg_store(),
    ok = rabbit_variable_queue:start_msg_store(
           undefined, {fun (ok) -> finished end, ok}).

msg_id_bin(X) ->
    erlang:md5(term_to_binary(X)).

on_disk_capture() ->
    receive
        {await, MsgIds, Pid} -> on_disk_capture([], MsgIds, Pid);
        stop                 -> done
    end.

on_disk_capture([_|_], _Awaiting, Pid) ->
    Pid ! {self(), surplus};
on_disk_capture(OnDisk, Awaiting, Pid) ->
    receive
        {on_disk, MsgIdsS} ->
            MsgIds = gb_sets:to_list(MsgIdsS),
            on_disk_capture(OnDisk ++ (MsgIds -- Awaiting), Awaiting -- MsgIds,
                            Pid);
        stop ->
            done
    after (case Awaiting of [] -> 200; _ -> ?TIMEOUT end) ->
            case Awaiting of
                [] -> Pid ! {self(), arrived}, on_disk_capture();
                _  -> Pid ! {self(), timeout}
            end
    end.

on_disk_await(Pid, MsgIds) when is_list(MsgIds) ->
    Pid ! {await, MsgIds, self()},
    receive
        {Pid, arrived} -> ok;
        {Pid, Error}   -> Error
    end.

on_disk_stop(Pid) ->
    MRef = erlang:monitor(process, Pid),
    Pid ! stop,
    receive {'DOWN', MRef, process, Pid, _Reason} ->
            ok
    end.

msg_store_client_init_capture(MsgStore, Ref) ->
    Pid = spawn(fun on_disk_capture/0),
    {Pid, rabbit_msg_store:client_init(
            MsgStore, Ref, fun (MsgIds, _ActionTaken) ->
                                   Pid ! {on_disk, MsgIds}
                           end, undefined)}.

msg_store_contains(Atom, MsgIds, MSCState) ->
    Atom = lists:foldl(
             fun (MsgId, Atom1) when Atom1 =:= Atom ->
                     rabbit_msg_store:contains(MsgId, MSCState) end,
             Atom, MsgIds).

msg_store_read(MsgIds, MSCState) ->
    lists:foldl(fun (MsgId, MSCStateM) ->
                        {{ok, MsgId}, MSCStateN} = rabbit_msg_store:read(
                                                     MsgId, MSCStateM),
                        MSCStateN
                end, MSCState, MsgIds).

msg_store_write(MsgIds, MSCState) ->
    ok = lists:foldl(fun (MsgId, ok) ->
                             rabbit_msg_store:write(MsgId, MsgId, MSCState)
                     end, ok, MsgIds).

msg_store_write_flow(MsgIds, MSCState) ->
    ok = lists:foldl(fun (MsgId, ok) ->
                             rabbit_msg_store:write_flow(MsgId, MsgId, MSCState)
                     end, ok, MsgIds).

msg_store_remove(MsgIds, MSCState) ->
    rabbit_msg_store:remove(MsgIds, MSCState).

msg_store_remove(MsgStore, Ref, MsgIds) ->
    with_msg_store_client(MsgStore, Ref,
                          fun (MSCStateM) ->
                                  ok = msg_store_remove(MsgIds, MSCStateM),
                                  MSCStateM
                          end).

with_msg_store_client(MsgStore, Ref, Fun) ->
    rabbit_msg_store:client_terminate(
      Fun(msg_store_client_init(MsgStore, Ref))).

foreach_with_msg_store_client(MsgStore, Ref, Fun, L) ->
    rabbit_msg_store:client_terminate(
      lists:foldl(fun (MsgId, MSCState) -> Fun(MsgId, MSCState) end,
                  msg_store_client_init(MsgStore, Ref), L)).

test_msg_store_confirms(MsgIds, Cap, MSCState) ->
    %% write -> confirmed
    ok = msg_store_write(MsgIds, MSCState),
    ok = on_disk_await(Cap, MsgIds),
    %% remove -> _
    ok = msg_store_remove(MsgIds, MSCState),
    ok = on_disk_await(Cap, []),
    %% write, remove -> confirmed
    ok = msg_store_write(MsgIds, MSCState),
    ok = msg_store_remove(MsgIds, MSCState),
    ok = on_disk_await(Cap, MsgIds),
    %% write, remove, write -> confirmed, confirmed
    ok = msg_store_write(MsgIds, MSCState),
    ok = msg_store_remove(MsgIds, MSCState),
    ok = msg_store_write(MsgIds, MSCState),
    ok = on_disk_await(Cap, MsgIds ++ MsgIds),
    %% remove, write -> confirmed
    ok = msg_store_remove(MsgIds, MSCState),
    ok = msg_store_write(MsgIds, MSCState),
    ok = on_disk_await(Cap, MsgIds),
    %% remove, write, remove -> confirmed
    ok = msg_store_remove(MsgIds, MSCState),
    ok = msg_store_write(MsgIds, MSCState),
    ok = msg_store_remove(MsgIds, MSCState),
    ok = on_disk_await(Cap, MsgIds),
    %% confirmation on timer-based sync
    passed = test_msg_store_confirm_timer(),
    passed.

test_msg_store_confirm_timer() ->
    Ref = rabbit_guid:gen(),
    MsgId  = msg_id_bin(1),
    Self = self(),
    MSCState = rabbit_msg_store:client_init(
                 ?PERSISTENT_MSG_STORE, Ref,
                 fun (MsgIds, _ActionTaken) ->
                         case gb_sets:is_member(MsgId, MsgIds) of
                             true  -> Self ! on_disk;
                             false -> ok
                         end
                 end, undefined),
    ok = msg_store_write([MsgId], MSCState),
    ok = msg_store_keep_busy_until_confirm([msg_id_bin(2)], MSCState, false),
    ok = msg_store_remove([MsgId], MSCState),
    ok = rabbit_msg_store:client_delete_and_terminate(MSCState),
    passed.

msg_store_keep_busy_until_confirm(MsgIds, MSCState, Blocked) ->
    After = case Blocked of
                false -> 0;
                true  -> ?MAX_WAIT
            end,
    Recurse = fun () -> msg_store_keep_busy_until_confirm(
                          MsgIds, MSCState, credit_flow:blocked()) end,
    receive
        on_disk            -> ok;
        {bump_credit, Msg} -> credit_flow:handle_bump_msg(Msg),
                              Recurse()
    after After ->
            ok = msg_store_write_flow(MsgIds, MSCState),
            ok = msg_store_remove(MsgIds, MSCState),
            Recurse()
    end.

test_msg_store_client_delete_and_terminate() ->
    restart_msg_store_empty(),
    MsgIds = [msg_id_bin(M) || M <- lists:seq(1, 10)],
    Ref = rabbit_guid:gen(),
    MSCState = msg_store_client_init(?PERSISTENT_MSG_STORE, Ref),
    ok = msg_store_write(MsgIds, MSCState),
    %% test the 'dying client' fast path for writes
    ok = rabbit_msg_store:client_delete_and_terminate(MSCState),
    passed.

%% -------------------------------------------------------------------
%% Backing queue.
%% -------------------------------------------------------------------

setup_backing_queue_test_group(Config) ->
    {ok, FileSizeLimit} =
        application:get_env(rabbit, msg_store_file_size_limit),
    application:set_env(rabbit, msg_store_file_size_limit, 512),
    {ok, MaxJournal} =
        application:get_env(rabbit, queue_index_max_journal_entries),
    application:set_env(rabbit, queue_index_max_journal_entries, 128),
    %% FIXME passed = test_msg_store(),
    application:set_env(rabbit, msg_store_file_size_limit,
                        FileSizeLimit),
    {ok, Bytes} =
        application:get_env(rabbit, queue_index_embed_msgs_below),
    rabbit_ct_helpers:set_config(Config, [
        {rmq_queue_index_max_journal_entries, MaxJournal},
        {rmq_queue_index_embed_msgs_below, Bytes}
      ]).

teardown_backing_queue_test_group(Config) ->
    %% FIXME: Undo all the setup function did.
    application:set_env(rabbit, queue_index_max_journal_entries,
                        ?config(rmq_queue_index_max_journal_entries, Config)),
    %% We will have restarted the message store, and thus changed
    %% the order of the children of rabbit_sup. This will cause
    %% problems if there are subsequent failures - see bug 24262.
    ok = restart_app(),
    Config.

bq_queue_index(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, bq_queue_index1, [Config]).

bq_queue_index1(_Config) ->
    SegmentSize = rabbit_queue_index:next_segment_boundary(0),
    TwoSegs = SegmentSize + SegmentSize,
    MostOfASegment = trunc(SegmentSize*0.75),
    SeqIdsA = lists:seq(0, MostOfASegment-1),
    SeqIdsB = lists:seq(MostOfASegment, 2*MostOfASegment),
    SeqIdsC = lists:seq(0, trunc(SegmentSize/2)),
    SeqIdsD = lists:seq(0, SegmentSize*4),

    with_empty_test_queue(
      fun (Qi0) ->
              {0, 0, Qi1} = rabbit_queue_index:bounds(Qi0),
              {Qi2, SeqIdsMsgIdsA} = queue_index_publish(SeqIdsA, false, Qi1),
              {0, SegmentSize, Qi3} = rabbit_queue_index:bounds(Qi2),
              {ReadA, Qi4} = rabbit_queue_index:read(0, SegmentSize, Qi3),
              ok = verify_read_with_published(false, false, ReadA,
                                              lists:reverse(SeqIdsMsgIdsA)),
              %% should get length back as 0, as all the msgs were transient
              {0, 0, Qi6} = restart_test_queue(Qi4),
              {0, 0, Qi7} = rabbit_queue_index:bounds(Qi6),
              {Qi8, SeqIdsMsgIdsB} = queue_index_publish(SeqIdsB, true, Qi7),
              {0, TwoSegs, Qi9} = rabbit_queue_index:bounds(Qi8),
              {ReadB, Qi10} = rabbit_queue_index:read(0, SegmentSize, Qi9),
              ok = verify_read_with_published(false, true, ReadB,
                                              lists:reverse(SeqIdsMsgIdsB)),
              %% should get length back as MostOfASegment
              LenB = length(SeqIdsB),
              BytesB = LenB * 10,
              {LenB, BytesB, Qi12} = restart_test_queue(Qi10),
              {0, TwoSegs, Qi13} = rabbit_queue_index:bounds(Qi12),
              Qi14 = rabbit_queue_index:deliver(SeqIdsB, Qi13),
              {ReadC, Qi15} = rabbit_queue_index:read(0, SegmentSize, Qi14),
              ok = verify_read_with_published(true, true, ReadC,
                                              lists:reverse(SeqIdsMsgIdsB)),
              Qi16 = rabbit_queue_index:ack(SeqIdsB, Qi15),
              Qi17 = rabbit_queue_index:flush(Qi16),
              %% Everything will have gone now because #pubs == #acks
              {0, 0, Qi18} = rabbit_queue_index:bounds(Qi17),
              %% should get length back as 0 because all persistent
              %% msgs have been acked
              {0, 0, Qi19} = restart_test_queue(Qi18),
              Qi19
      end),

    %% These next bits are just to hit the auto deletion of segment files.
    %% First, partials:
    %% a) partial pub+del+ack, then move to new segment
    with_empty_test_queue(
      fun (Qi0) ->
              {Qi1, _SeqIdsMsgIdsC} = queue_index_publish(SeqIdsC,
                                                          false, Qi0),
              Qi2 = rabbit_queue_index:deliver(SeqIdsC, Qi1),
              Qi3 = rabbit_queue_index:ack(SeqIdsC, Qi2),
              Qi4 = rabbit_queue_index:flush(Qi3),
              {Qi5, _SeqIdsMsgIdsC1} = queue_index_publish([SegmentSize],
                                                           false, Qi4),
              Qi5
      end),

    %% b) partial pub+del, then move to new segment, then ack all in old segment
    with_empty_test_queue(
      fun (Qi0) ->
              {Qi1, _SeqIdsMsgIdsC2} = queue_index_publish(SeqIdsC,
                                                           false, Qi0),
              Qi2 = rabbit_queue_index:deliver(SeqIdsC, Qi1),
              {Qi3, _SeqIdsMsgIdsC3} = queue_index_publish([SegmentSize],
                                                           false, Qi2),
              Qi4 = rabbit_queue_index:ack(SeqIdsC, Qi3),
              rabbit_queue_index:flush(Qi4)
      end),

    %% c) just fill up several segments of all pubs, then +dels, then +acks
    with_empty_test_queue(
      fun (Qi0) ->
              {Qi1, _SeqIdsMsgIdsD} = queue_index_publish(SeqIdsD,
                                                          false, Qi0),
              Qi2 = rabbit_queue_index:deliver(SeqIdsD, Qi1),
              Qi3 = rabbit_queue_index:ack(SeqIdsD, Qi2),
              rabbit_queue_index:flush(Qi3)
      end),

    %% d) get messages in all states to a segment, then flush, then do
    %% the same again, don't flush and read. This will hit all
    %% possibilities in combining the segment with the journal.
    with_empty_test_queue(
      fun (Qi0) ->
              {Qi1, [Seven,Five,Four|_]} = queue_index_publish([0,1,2,4,5,7],
                                                               false, Qi0),
              Qi2 = rabbit_queue_index:deliver([0,1,4], Qi1),
              Qi3 = rabbit_queue_index:ack([0], Qi2),
              Qi4 = rabbit_queue_index:flush(Qi3),
              {Qi5, [Eight,Six|_]} = queue_index_publish([3,6,8], false, Qi4),
              Qi6 = rabbit_queue_index:deliver([2,3,5,6], Qi5),
              Qi7 = rabbit_queue_index:ack([1,2,3], Qi6),
              {[], Qi8} = rabbit_queue_index:read(0, 4, Qi7),
              {ReadD, Qi9} = rabbit_queue_index:read(4, 7, Qi8),
              ok = verify_read_with_published(true, false, ReadD,
                                              [Four, Five, Six]),
              {ReadE, Qi10} = rabbit_queue_index:read(7, 9, Qi9),
              ok = verify_read_with_published(false, false, ReadE,
                                              [Seven, Eight]),
              Qi10
      end),

    %% e) as for (d), but use terminate instead of read, which will
    %% exercise journal_minus_segment, not segment_plus_journal.
    with_empty_test_queue(
      fun (Qi0) ->
              {Qi1, _SeqIdsMsgIdsE} = queue_index_publish([0,1,2,4,5,7],
                                                          true, Qi0),
              Qi2 = rabbit_queue_index:deliver([0,1,4], Qi1),
              Qi3 = rabbit_queue_index:ack([0], Qi2),
              {5, 50, Qi4} = restart_test_queue(Qi3),
              {Qi5, _SeqIdsMsgIdsF} = queue_index_publish([3,6,8], true, Qi4),
              Qi6 = rabbit_queue_index:deliver([2,3,5,6], Qi5),
              Qi7 = rabbit_queue_index:ack([1,2,3], Qi6),
              {5, 50, Qi8} = restart_test_queue(Qi7),
              Qi8
      end),

    ok = rabbit_variable_queue:stop(),
    {ok, _} = rabbit_variable_queue:start([]),

    passed.

bq_queue_index_props(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, bq_queue_index_props1, [Config]).

bq_queue_index_props1(_Config) ->
    with_empty_test_queue(
      fun(Qi0) ->
              MsgId = rabbit_guid:gen(),
              Props = #message_properties{expiry=12345, size = 10},
              Qi1 = rabbit_queue_index:publish(
                      MsgId, 1, Props, true, infinity, Qi0),
              {[{MsgId, 1, Props, _, _}], Qi2} =
                  rabbit_queue_index:read(1, 2, Qi1),
              Qi2
      end),

    ok = rabbit_variable_queue:stop(),
    {ok, _} = rabbit_variable_queue:start([]),

    passed.

bq_variable_queue_delete_msg_store_files_callback(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, bq_variable_queue_delete_msg_store_files_callback1, [Config]).

bq_variable_queue_delete_msg_store_files_callback1(_Config) ->
    ok = restart_msg_store_empty(),
    {new, #amqqueue { pid = QPid, name = QName } = Q} =
        rabbit_amqqueue:declare(test_queue(), true, false, [], none),
    Payload = <<0:8388608>>, %% 1MB
    Count = 30,
    publish_and_confirm(Q, Payload, Count),

    rabbit_amqqueue:set_ram_duration_target(QPid, 0),

    {ok, Limiter} = rabbit_limiter:start_link(no_id),

    CountMinusOne = Count - 1,
    {ok, CountMinusOne, {QName, QPid, _AckTag, false, _Msg}} =
        rabbit_amqqueue:basic_get(Q, self(), true, Limiter),
    {ok, CountMinusOne} = rabbit_amqqueue:purge(Q),

    %% give the queue a second to receive the close_fds callback msg
    timer:sleep(1000),

    rabbit_amqqueue:delete(Q, false, false),
    passed.

bq_queue_recover(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, bq_queue_recover1, [Config]).

bq_queue_recover1(_Config) ->
    Count = 2 * rabbit_queue_index:next_segment_boundary(0),
    {new, #amqqueue { pid = QPid, name = QName } = Q} =
        rabbit_amqqueue:declare(test_queue(), true, false, [], none),
    publish_and_confirm(Q, <<>>, Count),

    [{_, SupPid, _, _}] = supervisor:which_children(rabbit_amqqueue_sup_sup),
    exit(SupPid, kill),
    exit(QPid, kill),
    MRef = erlang:monitor(process, QPid),
    receive {'DOWN', MRef, process, QPid, _Info} -> ok
    after 10000 -> exit(timeout_waiting_for_queue_death)
    end,
    rabbit_amqqueue:stop(),
    rabbit_amqqueue:start(rabbit_amqqueue:recover()),
    {ok, Limiter} = rabbit_limiter:start_link(no_id),
    rabbit_amqqueue:with_or_die(
      QName,
      fun (Q1 = #amqqueue { pid = QPid1 }) ->
              CountMinusOne = Count - 1,
              {ok, CountMinusOne, {QName, QPid1, _AckTag, true, _Msg}} =
                  rabbit_amqqueue:basic_get(Q1, self(), false, Limiter),
              exit(QPid1, shutdown),
              VQ1 = variable_queue_init(Q, true),
              {{_Msg1, true, _AckTag1}, VQ2} =
                  rabbit_variable_queue:fetch(true, VQ1),
              CountMinusOne = rabbit_variable_queue:len(VQ2),
              _VQ3 = rabbit_variable_queue:delete_and_terminate(shutdown, VQ2),
              rabbit_amqqueue:internal_delete(QName)
      end),
    passed.

variable_queue_dynamic_duration_change(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, variable_queue_dynamic_duration_change1, [Config]).

variable_queue_dynamic_duration_change1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_dynamic_duration_change2/1,
      ?config(variable_queue_type, Config)).

variable_queue_dynamic_duration_change2(VQ0) ->
    SegmentSize = rabbit_queue_index:next_segment_boundary(0),

    %% start by sending in a couple of segments worth
    Len = 2*SegmentSize,
    VQ1 = variable_queue_publish(false, Len, VQ0),
    %% squeeze and relax queue
    Churn = Len div 32,
    VQ2 = publish_fetch_and_ack(Churn, Len, VQ1),

    {Duration, VQ3} = rabbit_variable_queue:ram_duration(VQ2),
    VQ7 = lists:foldl(
            fun (Duration1, VQ4) ->
                    {_Duration, VQ5} = rabbit_variable_queue:ram_duration(VQ4),
                    VQ6 = variable_queue_set_ram_duration_target(
                            Duration1, VQ5),
                    publish_fetch_and_ack(Churn, Len, VQ6)
            end, VQ3, [Duration / 4, 0, Duration / 4, infinity]),

    %% drain
    {VQ8, AckTags} = variable_queue_fetch(Len, false, false, Len, VQ7),
    {_Guids, VQ9} = rabbit_variable_queue:ack(AckTags, VQ8),
    {empty, VQ10} = rabbit_variable_queue:fetch(true, VQ9),

    VQ10.

variable_queue_partial_segments_delta_thing(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, variable_queue_partial_segments_delta_thing1, [Config]).

variable_queue_partial_segments_delta_thing1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_partial_segments_delta_thing2/1,
      ?config(variable_queue_type, Config)).

variable_queue_partial_segments_delta_thing2(VQ0) ->
    SegmentSize = rabbit_queue_index:next_segment_boundary(0),
    HalfSegment = SegmentSize div 2,
    OneAndAHalfSegment = SegmentSize + HalfSegment,
    VQ1 = variable_queue_publish(true, OneAndAHalfSegment, VQ0),
    {_Duration, VQ2} = rabbit_variable_queue:ram_duration(VQ1),
    VQ3 = check_variable_queue_status(
            variable_queue_set_ram_duration_target(0, VQ2),
            %% one segment in q3, and half a segment in delta
            [{delta, {delta, SegmentSize, HalfSegment, OneAndAHalfSegment}},
             {q3, SegmentSize},
             {len, SegmentSize + HalfSegment}]),
    VQ4 = variable_queue_set_ram_duration_target(infinity, VQ3),
    VQ5 = check_variable_queue_status(
            variable_queue_publish(true, 1, VQ4),
            %% one alpha, but it's in the same segment as the deltas
            [{q1, 1},
             {delta, {delta, SegmentSize, HalfSegment, OneAndAHalfSegment}},
             {q3, SegmentSize},
             {len, SegmentSize + HalfSegment + 1}]),
    {VQ6, AckTags} = variable_queue_fetch(SegmentSize, true, false,
                                          SegmentSize + HalfSegment + 1, VQ5),
    VQ7 = check_variable_queue_status(
            VQ6,
            %% the half segment should now be in q3
            [{q1, 1},
             {delta, {delta, undefined, 0, undefined}},
             {q3, HalfSegment},
             {len, HalfSegment + 1}]),
    {VQ8, AckTags1} = variable_queue_fetch(HalfSegment + 1, true, false,
                                           HalfSegment + 1, VQ7),
    {_Guids, VQ9} = rabbit_variable_queue:ack(AckTags ++ AckTags1, VQ8),
    %% should be empty now
    {empty, VQ10} = rabbit_variable_queue:fetch(true, VQ9),
    VQ10.

variable_queue_all_the_bits_not_covered_elsewhere_A(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, variable_queue_all_the_bits_not_covered_elsewhere_A1, [Config]).

variable_queue_all_the_bits_not_covered_elsewhere_A1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_all_the_bits_not_covered_elsewhere_A2/1,
      ?config(variable_queue_type, Config)).

variable_queue_all_the_bits_not_covered_elsewhere_A2(VQ0) ->
    Count = 2 * rabbit_queue_index:next_segment_boundary(0),
    VQ1 = variable_queue_publish(true, Count, VQ0),
    VQ2 = variable_queue_publish(false, Count, VQ1),
    VQ3 = variable_queue_set_ram_duration_target(0, VQ2),
    {VQ4, _AckTags}  = variable_queue_fetch(Count, true, false,
                                            Count + Count, VQ3),
    {VQ5, _AckTags1} = variable_queue_fetch(Count, false, false,
                                            Count, VQ4),
    _VQ6 = rabbit_variable_queue:terminate(shutdown, VQ5),
    VQ7 = variable_queue_init(test_amqqueue(true), true),
    {{_Msg1, true, _AckTag1}, VQ8} = rabbit_variable_queue:fetch(true, VQ7),
    Count1 = rabbit_variable_queue:len(VQ8),
    VQ9 = variable_queue_publish(false, 1, VQ8),
    VQ10 = variable_queue_set_ram_duration_target(0, VQ9),
    {VQ11, _AckTags2} = variable_queue_fetch(Count1, true, true, Count, VQ10),
    {VQ12, _AckTags3} = variable_queue_fetch(1, false, false, 1, VQ11),
    VQ12.

variable_queue_all_the_bits_not_covered_elsewhere_B(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, variable_queue_all_the_bits_not_covered_elsewhere_B1, [Config]).

variable_queue_all_the_bits_not_covered_elsewhere_B1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_all_the_bits_not_covered_elsewhere_B2/1,
      ?config(variable_queue_type, Config)).

variable_queue_all_the_bits_not_covered_elsewhere_B2(VQ0) ->
    VQ1 = variable_queue_set_ram_duration_target(0, VQ0),
    VQ2 = variable_queue_publish(false, 4, VQ1),
    {VQ3, AckTags} = variable_queue_fetch(2, false, false, 4, VQ2),
    {_Guids, VQ4} =
        rabbit_variable_queue:requeue(AckTags, VQ3),
    VQ5 = rabbit_variable_queue:timeout(VQ4),
    _VQ6 = rabbit_variable_queue:terminate(shutdown, VQ5),
    VQ7 = variable_queue_init(test_amqqueue(true), true),
    {empty, VQ8} = rabbit_variable_queue:fetch(false, VQ7),
    VQ8.

variable_queue_drop(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, variable_queue_drop1, [Config]).

variable_queue_drop1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_drop2/1,
      ?config(variable_queue_type, Config)).

variable_queue_drop2(VQ0) ->
    %% start by sending a messages
    VQ1 = variable_queue_publish(false, 1, VQ0),
    %% drop message with AckRequired = true
    {{MsgId, AckTag}, VQ2} = rabbit_variable_queue:drop(true, VQ1),
    true = rabbit_variable_queue:is_empty(VQ2),
    true = AckTag =/= undefinded,
    %% drop again -> empty
    {empty, VQ3} = rabbit_variable_queue:drop(false, VQ2),
    %% requeue
    {[MsgId], VQ4} = rabbit_variable_queue:requeue([AckTag], VQ3),
    %% drop message with AckRequired = false
    {{MsgId, undefined}, VQ5} = rabbit_variable_queue:drop(false, VQ4),
    true = rabbit_variable_queue:is_empty(VQ5),
    VQ5.

variable_queue_fold_msg_on_disk(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, variable_queue_fold_msg_on_disk1, [Config]).

variable_queue_fold_msg_on_disk1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_fold_msg_on_disk2/1,
      ?config(variable_queue_type, Config)).

variable_queue_fold_msg_on_disk2(VQ0) ->
    VQ1 = variable_queue_publish(true, 1, VQ0),
    {VQ2, AckTags} = variable_queue_fetch(1, true, false, 1, VQ1),
    {ok, VQ3} = rabbit_variable_queue:ackfold(fun (_M, _A, ok) -> ok end,
                                              ok, VQ2, AckTags),
    VQ3.

variable_queue_dropfetchwhile(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, variable_queue_dropfetchwhile1, [Config]).

variable_queue_dropfetchwhile1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_dropfetchwhile2/1,
      ?config(variable_queue_type, Config)).

variable_queue_dropfetchwhile2(VQ0) ->
    Count = 10,

    %% add messages with sequential expiry
    VQ1 = variable_queue_publish(
            false, 1, Count,
            fun (N, Props) -> Props#message_properties{expiry = N} end,
            fun erlang:term_to_binary/1, VQ0),

    %% fetch the first 5 messages
    {#message_properties{expiry = 6}, {Msgs, AckTags}, VQ2} =
        rabbit_variable_queue:fetchwhile(
          fun (#message_properties{expiry = Expiry}) -> Expiry =< 5 end,
          fun (Msg, AckTag, {MsgAcc, AckAcc}) ->
                  {[Msg | MsgAcc], [AckTag | AckAcc]}
          end, {[], []}, VQ1),
    true = lists:seq(1, 5) == [msg2int(M) || M <- lists:reverse(Msgs)],

    %% requeue them
    {_MsgIds, VQ3} = rabbit_variable_queue:requeue(AckTags, VQ2),

    %% drop the first 5 messages
    {#message_properties{expiry = 6}, VQ4} =
        rabbit_variable_queue:dropwhile(
          fun (#message_properties {expiry = Expiry}) -> Expiry =< 5 end, VQ3),

    %% fetch 5
    VQ5 = lists:foldl(fun (N, VQN) ->
                              {{Msg, _, _}, VQM} =
                                  rabbit_variable_queue:fetch(false, VQN),
                              true = msg2int(Msg) == N,
                              VQM
                      end, VQ4, lists:seq(6, Count)),

    %% should be empty now
    true = rabbit_variable_queue:is_empty(VQ5),

    VQ5.

variable_queue_dropwhile_varying_ram_duration(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, variable_queue_dropwhile_varying_ram_duration1, [Config]).

variable_queue_dropwhile_varying_ram_duration1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_dropwhile_varying_ram_duration2/1,
      ?config(variable_queue_type, Config)).

variable_queue_dropwhile_varying_ram_duration2(VQ0) ->
    test_dropfetchwhile_varying_ram_duration(
      fun (VQ1) ->
              {_, VQ2} = rabbit_variable_queue:dropwhile(
                           fun (_) -> false end, VQ1),
              VQ2
      end, VQ0).

variable_queue_fetchwhile_varying_ram_duration(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, variable_queue_fetchwhile_varying_ram_duration1, [Config]).

variable_queue_fetchwhile_varying_ram_duration1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_fetchwhile_varying_ram_duration2/1,
      ?config(variable_queue_type, Config)).

variable_queue_fetchwhile_varying_ram_duration2(VQ0) ->
    test_dropfetchwhile_varying_ram_duration(
      fun (VQ1) ->
              {_, ok, VQ2} = rabbit_variable_queue:fetchwhile(
                               fun (_) -> false end,
                               fun (_, _, A) -> A end,
                               ok, VQ1),
              VQ2
      end, VQ0).

test_dropfetchwhile_varying_ram_duration(Fun, VQ0) ->
    VQ1 = variable_queue_publish(false, 1, VQ0),
    VQ2 = variable_queue_set_ram_duration_target(0, VQ1),
    VQ3 = Fun(VQ2),
    VQ4 = variable_queue_set_ram_duration_target(infinity, VQ3),
    VQ5 = variable_queue_publish(false, 1, VQ4),
    VQ6 = Fun(VQ5),
    VQ6.

variable_queue_ack_limiting(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, variable_queue_ack_limiting1, [Config]).

variable_queue_ack_limiting1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_ack_limiting2/1,
      ?config(variable_queue_type, Config)).

variable_queue_ack_limiting2(VQ0) ->
    %% start by sending in a bunch of messages
    Len = 1024,
    VQ1 = variable_queue_publish(false, Len, VQ0),

    %% squeeze and relax queue
    Churn = Len div 32,
    VQ2 = publish_fetch_and_ack(Churn, Len, VQ1),

    %% update stats for duration
    {_Duration, VQ3} = rabbit_variable_queue:ram_duration(VQ2),

    %% fetch half the messages
    {VQ4, _AckTags} = variable_queue_fetch(Len div 2, false, false, Len, VQ3),

    VQ5 = check_variable_queue_status(
            VQ4, [{len,                         Len div 2},
                  {messages_unacknowledged_ram, Len div 2},
                  {messages_ready_ram,          Len div 2},
                  {messages_ram,                Len}]),

    %% ensure all acks go to disk on 0 duration target
    VQ6 = check_variable_queue_status(
            variable_queue_set_ram_duration_target(0, VQ5),
            [{len,                         Len div 2},
             {target_ram_count,            0},
             {messages_unacknowledged_ram, 0},
             {messages_ready_ram,          0},
             {messages_ram,                0}]),

    VQ6.

variable_queue_purge(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, variable_queue_purge1, [Config]).

variable_queue_purge1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_purge2/1,
      ?config(variable_queue_type, Config)).

variable_queue_purge2(VQ0) ->
    LenDepth = fun (VQ) ->
                       {rabbit_variable_queue:len(VQ),
                        rabbit_variable_queue:depth(VQ)}
               end,
    VQ1         = variable_queue_publish(false, 10, VQ0),
    {VQ2, Acks} = variable_queue_fetch(6, false, false, 10, VQ1),
    {4, VQ3}    = rabbit_variable_queue:purge(VQ2),
    {0, 6}      = LenDepth(VQ3),
    {_, VQ4}    = rabbit_variable_queue:requeue(lists:sublist(Acks, 2), VQ3),
    {2, 6}      = LenDepth(VQ4),
    VQ5         = rabbit_variable_queue:purge_acks(VQ4),
    {2, 2}      = LenDepth(VQ5),
    VQ5.

variable_queue_requeue(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, variable_queue_requeue1, [Config]).

variable_queue_requeue1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_requeue2/1,
      ?config(variable_queue_type, Config)).

variable_queue_requeue2(VQ0) ->
    {_PendingMsgs, RequeuedMsgs, FreshMsgs, VQ1} =
        variable_queue_with_holes(VQ0),
    Msgs =
        lists:zip(RequeuedMsgs,
                  lists:duplicate(length(RequeuedMsgs), true)) ++
        lists:zip(FreshMsgs,
                  lists:duplicate(length(FreshMsgs), false)),
    VQ2 = lists:foldl(fun ({I, Requeued}, VQa) ->
                              {{M, MRequeued, _}, VQb} =
                                  rabbit_variable_queue:fetch(true, VQa),
                              Requeued = MRequeued, %% assertion
                              I = msg2int(M),       %% assertion
                              VQb
                      end, VQ1, Msgs),
    {empty, VQ3} = rabbit_variable_queue:fetch(true, VQ2),
    VQ3.

%% requeue from ram_pending_ack into q3, move to delta and then empty queue
variable_queue_requeue_ram_beta(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, variable_queue_requeue_ram_beta1, [Config]).

variable_queue_requeue_ram_beta1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_requeue_ram_beta2/1,
      ?config(variable_queue_type, Config)).

variable_queue_requeue_ram_beta2(VQ0) ->
    Count = rabbit_queue_index:next_segment_boundary(0)*2 + 2,
    VQ1 = variable_queue_publish(false, Count, VQ0),
    {VQ2, AcksR} = variable_queue_fetch(Count, false, false, Count, VQ1),
    {Back, Front} = lists:split(Count div 2, AcksR),
    {_, VQ3} = rabbit_variable_queue:requeue(erlang:tl(Back), VQ2),
    VQ4 = variable_queue_set_ram_duration_target(0, VQ3),
    {_, VQ5} = rabbit_variable_queue:requeue([erlang:hd(Back)], VQ4),
    VQ6 = requeue_one_by_one(Front, VQ5),
    {VQ7, AcksAll} = variable_queue_fetch(Count, false, true, Count, VQ6),
    {_, VQ8} = rabbit_variable_queue:ack(AcksAll, VQ7),
    VQ8.

variable_queue_fold(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, variable_queue_fold1, [Config]).

variable_queue_fold1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_fold2/1,
      ?config(variable_queue_type, Config)).

variable_queue_fold2(VQ0) ->
    {PendingMsgs, RequeuedMsgs, FreshMsgs, VQ1} =
        variable_queue_with_holes(VQ0),
    Count = rabbit_variable_queue:depth(VQ1),
    Msgs = lists:sort(PendingMsgs ++ RequeuedMsgs ++ FreshMsgs),
    lists:foldl(fun (Cut, VQ2) ->
                        test_variable_queue_fold(Cut, Msgs, PendingMsgs, VQ2)
                end, VQ1, [0, 1, 2, Count div 2,
                           Count - 1, Count, Count + 1, Count * 2]).

test_variable_queue_fold(Cut, Msgs, PendingMsgs, VQ0) ->
    {Acc, VQ1} = rabbit_variable_queue:fold(
                   fun (M, _, Pending, A) ->
                           MInt = msg2int(M),
                           Pending = lists:member(MInt, PendingMsgs), %% assert
                           case MInt =< Cut of
                               true  -> {cont, [MInt | A]};
                               false -> {stop, A}
                           end
                   end, [], VQ0),
    Expected = lists:takewhile(fun (I) -> I =< Cut end, Msgs),
    Expected = lists:reverse(Acc), %% assertion
    VQ1.

variable_queue_batch_publish(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, variable_queue_batch_publish1, [Config]).

variable_queue_batch_publish1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_batch_publish2/1,
      ?config(variable_queue_type, Config)).

variable_queue_batch_publish2(VQ) ->
    Count = 10,
    VQ1 = variable_queue_batch_publish(true, Count, VQ),
    Count = rabbit_variable_queue:len(VQ1),
    VQ1.

variable_queue_batch_publish_delivered(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, variable_queue_batch_publish_delivered1, [Config]).

variable_queue_batch_publish_delivered1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_batch_publish_delivered2/1,
      ?config(variable_queue_type, Config)).

variable_queue_batch_publish_delivered2(VQ) ->
    Count = 10,
    VQ1 = variable_queue_batch_publish_delivered(true, Count, VQ),
    Count = rabbit_variable_queue:depth(VQ1),
    VQ1.

%% same as test_variable_queue_requeue_ram_beta but randomly changing
%% the queue mode after every step.
variable_queue_mode_change(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, variable_queue_mode_change1, [Config]).

variable_queue_mode_change1(Config) ->
    with_fresh_variable_queue(
      fun variable_queue_mode_change2/1,
      ?config(variable_queue_type, Config)).

variable_queue_mode_change2(VQ0) ->
    Count = rabbit_queue_index:next_segment_boundary(0)*2 + 2,
    VQ1 = variable_queue_publish(false, Count, VQ0),
    VQ2 = maybe_switch_queue_mode(VQ1),
    {VQ3, AcksR} = variable_queue_fetch(Count, false, false, Count, VQ2),
    VQ4 = maybe_switch_queue_mode(VQ3),
    {Back, Front} = lists:split(Count div 2, AcksR),
    {_, VQ5} = rabbit_variable_queue:requeue(erlang:tl(Back), VQ4),
    VQ6 = maybe_switch_queue_mode(VQ5),
    VQ7 = variable_queue_set_ram_duration_target(0, VQ6),
    VQ8 = maybe_switch_queue_mode(VQ7),
    {_, VQ9} = rabbit_variable_queue:requeue([erlang:hd(Back)], VQ8),
    VQ10 = maybe_switch_queue_mode(VQ9),
    VQ11 = requeue_one_by_one(Front, VQ10),
    VQ12 = maybe_switch_queue_mode(VQ11),
    {VQ13, AcksAll} = variable_queue_fetch(Count, false, true, Count, VQ12),
    VQ14 = maybe_switch_queue_mode(VQ13),
    {_, VQ15} = rabbit_variable_queue:ack(AcksAll, VQ14),
    VQ16 = maybe_switch_queue_mode(VQ15),
    VQ16.

maybe_switch_queue_mode(VQ) ->
    Mode = random_queue_mode(),
    set_queue_mode(Mode, VQ).

random_queue_mode() ->
    Modes = [lazy, default],
    lists:nth(random:uniform(length(Modes)), Modes).

pub_res({_, VQS}) ->
    VQS;
pub_res(VQS) ->
    VQS.

make_publish(IsPersistent, PayloadFun, PropFun, N) ->
    {rabbit_basic:message(
       rabbit_misc:r(<<>>, exchange, <<>>),
       <<>>, #'P_basic'{delivery_mode = case IsPersistent of
                                            true  -> 2;
                                            false -> 1
                                        end},
       PayloadFun(N)),
     PropFun(N, #message_properties{size = 10}),
     false}.

make_publish_delivered(IsPersistent, PayloadFun, PropFun, N) ->
    {rabbit_basic:message(
       rabbit_misc:r(<<>>, exchange, <<>>),
       <<>>, #'P_basic'{delivery_mode = case IsPersistent of
                                            true  -> 2;
                                            false -> 1
                                        end},
       PayloadFun(N)),
     PropFun(N, #message_properties{size = 10})}.

queue_name(Name) ->
    rabbit_misc:r(<<"/">>, queue, Name).

test_queue() ->
    queue_name(<<"test">>).

init_test_queue() ->
    TestQueue = test_queue(),
    PRef = rabbit_guid:gen(),
    PersistentClient = msg_store_client_init(?PERSISTENT_MSG_STORE, PRef),
    Res = rabbit_queue_index:recover(
            TestQueue, [], false,
            fun (MsgId) ->
                    rabbit_msg_store:contains(MsgId, PersistentClient)
            end,
            fun nop/1, fun nop/1),
    ok = rabbit_msg_store:client_delete_and_terminate(PersistentClient),
    Res.

restart_test_queue(Qi) ->
    _ = rabbit_queue_index:terminate([], Qi),
    ok = rabbit_variable_queue:stop(),
    {ok, _} = rabbit_variable_queue:start([test_queue()]),
    init_test_queue().

empty_test_queue() ->
    ok = rabbit_variable_queue:stop(),
    {ok, _} = rabbit_variable_queue:start([]),
    {0, 0, Qi} = init_test_queue(),
    _ = rabbit_queue_index:delete_and_terminate(Qi),
    ok.

with_empty_test_queue(Fun) ->
    ok = empty_test_queue(),
    {0, 0, Qi} = init_test_queue(),
    rabbit_queue_index:delete_and_terminate(Fun(Qi)).

restart_app() ->
    rabbit:stop(),
    rabbit:start().

queue_index_publish(SeqIds, Persistent, Qi) ->
    Ref = rabbit_guid:gen(),
    MsgStore = case Persistent of
                   true  -> ?PERSISTENT_MSG_STORE;
                   false -> ?TRANSIENT_MSG_STORE
               end,
    MSCState = msg_store_client_init(MsgStore, Ref),
    {A, B = [{_SeqId, LastMsgIdWritten} | _]} =
        lists:foldl(
          fun (SeqId, {QiN, SeqIdsMsgIdsAcc}) ->
                  MsgId = rabbit_guid:gen(),
                  QiM = rabbit_queue_index:publish(
                          MsgId, SeqId, #message_properties{size = 10},
                          Persistent, infinity, QiN),
                  ok = rabbit_msg_store:write(MsgId, MsgId, MSCState),
                  {QiM, [{SeqId, MsgId} | SeqIdsMsgIdsAcc]}
          end, {Qi, []}, SeqIds),
    %% do this just to force all of the publishes through to the msg_store:
    true = rabbit_msg_store:contains(LastMsgIdWritten, MSCState),
    ok = rabbit_msg_store:client_delete_and_terminate(MSCState),
    {A, B}.

verify_read_with_published(_Delivered, _Persistent, [], _) ->
    ok;
verify_read_with_published(Delivered, Persistent,
                           [{MsgId, SeqId, _Props, Persistent, Delivered}|Read],
                           [{SeqId, MsgId}|Published]) ->
    verify_read_with_published(Delivered, Persistent, Read, Published);
verify_read_with_published(_Delivered, _Persistent, _Read, _Published) ->
    ko.

nop(_) -> ok.
nop(_, _) -> ok.

msg_store_client_init(MsgStore, Ref) ->
    rabbit_msg_store:client_init(MsgStore, Ref, undefined, undefined).

variable_queue_init(Q, Recover) ->
    rabbit_variable_queue:init(
      Q, case Recover of
             true  -> non_clean_shutdown;
             false -> new
         end, fun nop/2, fun nop/2, fun nop/1, fun nop/1).

publish_and_confirm(Q, Payload, Count) ->
    Seqs = lists:seq(1, Count),
    [begin
         Msg = rabbit_basic:message(rabbit_misc:r(<<>>, exchange, <<>>),
                                    <<>>, #'P_basic'{delivery_mode = 2},
                                    Payload),
         Delivery = #delivery{mandatory = false, sender = self(),
                              confirm = true, message = Msg, msg_seq_no = Seq,
                              flow = noflow},
         _QPids = rabbit_amqqueue:deliver([Q], Delivery)
     end || Seq <- Seqs],
    wait_for_confirms(gb_sets:from_list(Seqs)).

wait_for_confirms(Unconfirmed) ->
    case gb_sets:is_empty(Unconfirmed) of
        true  -> ok;
        false -> receive {'$gen_cast', {confirm, Confirmed, _}} ->
                         wait_for_confirms(
                           rabbit_misc:gb_sets_difference(
                             Unconfirmed, gb_sets:from_list(Confirmed)))
                 after ?TIMEOUT -> exit(timeout_waiting_for_confirm)
                 end
    end.

with_fresh_variable_queue(Fun, Mode) ->
    Ref = make_ref(),
    Me = self(),
    %% Run in a separate process since rabbit_msg_store will send
    %% bump_credit messages and we want to ignore them
    spawn_link(fun() ->
                       ok = empty_test_queue(),
                       VQ = variable_queue_init(test_amqqueue(true), false),
                       S0 = variable_queue_status(VQ),
                       assert_props(S0, [{q1, 0}, {q2, 0},
                                         {delta,
                                          {delta, undefined, 0, undefined}},
                                         {q3, 0}, {q4, 0},
                                         {len, 0}]),
                       VQ1 = set_queue_mode(Mode, VQ),
                       try
                           _ = rabbit_variable_queue:delete_and_terminate(
                                 shutdown, Fun(VQ1)),
                           Me ! Ref
                       catch
                           Type:Error ->
                               Me ! {Ref, Type, Error, erlang:get_stacktrace()}
                       end
               end),
    receive
        Ref                    -> ok;
        {Ref, Type, Error, ST} -> exit({Type, Error, ST})
    end,
    passed.

set_queue_mode(Mode, VQ) ->
    VQ1 = rabbit_variable_queue:set_queue_mode(Mode, VQ),
    S1 = variable_queue_status(VQ1),
    assert_props(S1, [{mode, Mode}]),
    VQ1.

variable_queue_publish(IsPersistent, Count, VQ) ->
    variable_queue_publish(IsPersistent, Count, fun (_N, P) -> P end, VQ).

variable_queue_publish(IsPersistent, Count, PropFun, VQ) ->
    variable_queue_publish(IsPersistent, 1, Count, PropFun,
                           fun (_N) -> <<>> end, VQ).

variable_queue_publish(IsPersistent, Start, Count, PropFun, PayloadFun, VQ) ->
    variable_queue_wait_for_shuffling_end(
      lists:foldl(
        fun (N, VQN) ->
                rabbit_variable_queue:publish(
                  rabbit_basic:message(
                    rabbit_misc:r(<<>>, exchange, <<>>),
                    <<>>, #'P_basic'{delivery_mode = case IsPersistent of
                                                         true  -> 2;
                                                         false -> 1
                                                     end},
                    PayloadFun(N)),
                  PropFun(N, #message_properties{size = 10}),
                  false, self(), noflow, VQN)
        end, VQ, lists:seq(Start, Start + Count - 1))).

variable_queue_batch_publish(IsPersistent, Count, VQ) ->
    variable_queue_batch_publish(IsPersistent, Count, fun (_N, P) -> P end, VQ).

variable_queue_batch_publish(IsPersistent, Count, PropFun, VQ) ->
    variable_queue_batch_publish(IsPersistent, 1, Count, PropFun,
                                 fun (_N) -> <<>> end, VQ).

variable_queue_batch_publish(IsPersistent, Start, Count, PropFun, PayloadFun, VQ) ->
    variable_queue_batch_publish0(IsPersistent, Start, Count, PropFun,
                                  PayloadFun, fun make_publish/4,
                                  fun rabbit_variable_queue:batch_publish/4,
                                  VQ).

variable_queue_batch_publish_delivered(IsPersistent, Count, VQ) ->
    variable_queue_batch_publish_delivered(IsPersistent, Count, fun (_N, P) -> P end, VQ).

variable_queue_batch_publish_delivered(IsPersistent, Count, PropFun, VQ) ->
    variable_queue_batch_publish_delivered(IsPersistent, 1, Count, PropFun,
                                           fun (_N) -> <<>> end, VQ).

variable_queue_batch_publish_delivered(IsPersistent, Start, Count, PropFun, PayloadFun, VQ) ->
    variable_queue_batch_publish0(IsPersistent, Start, Count, PropFun,
                                  PayloadFun, fun make_publish_delivered/4,
                                  fun rabbit_variable_queue:batch_publish_delivered/4,
                                  VQ).

variable_queue_batch_publish0(IsPersistent, Start, Count, PropFun, PayloadFun,
                              MakePubFun, PubFun, VQ) ->
    Publishes =
        [MakePubFun(IsPersistent, PayloadFun, PropFun, N)
         || N <- lists:seq(Start, Start + Count - 1)],
    Res = PubFun(Publishes, self(), noflow, VQ),
    VQ1 = pub_res(Res),
    variable_queue_wait_for_shuffling_end(VQ1).

variable_queue_fetch(Count, IsPersistent, IsDelivered, Len, VQ) ->
    lists:foldl(fun (N, {VQN, AckTagsAcc}) ->
                        Rem = Len - N,
                        {{#basic_message { is_persistent = IsPersistent },
                          IsDelivered, AckTagN}, VQM} =
                            rabbit_variable_queue:fetch(true, VQN),
                        Rem = rabbit_variable_queue:len(VQM),
                        {VQM, [AckTagN | AckTagsAcc]}
                end, {VQ, []}, lists:seq(1, Count)).

test_amqqueue(Durable) ->
    (rabbit_amqqueue:pseudo_queue(test_queue(), self()))
        #amqqueue { durable = Durable }.

assert_prop(List, Prop, Value) ->
    case proplists:get_value(Prop, List)of
        Value -> ok;
        _     -> {exit, Prop, exp, Value, List}
    end.

assert_props(List, PropVals) ->
    [assert_prop(List, Prop, Value) || {Prop, Value} <- PropVals].

variable_queue_set_ram_duration_target(Duration, VQ) ->
    variable_queue_wait_for_shuffling_end(
      rabbit_variable_queue:set_ram_duration_target(Duration, VQ)).

publish_fetch_and_ack(0, _Len, VQ0) ->
    VQ0;
publish_fetch_and_ack(N, Len, VQ0) ->
    VQ1 = variable_queue_publish(false, 1, VQ0),
    {{_Msg, false, AckTag}, VQ2} = rabbit_variable_queue:fetch(true, VQ1),
    Len = rabbit_variable_queue:len(VQ2),
    {_Guids, VQ3} = rabbit_variable_queue:ack([AckTag], VQ2),
    publish_fetch_and_ack(N-1, Len, VQ3).

variable_queue_status(VQ) ->
    Keys = rabbit_backing_queue:info_keys() -- [backing_queue_status],
    [{K, rabbit_variable_queue:info(K, VQ)} || K <- Keys] ++
        rabbit_variable_queue:info(backing_queue_status, VQ).

variable_queue_wait_for_shuffling_end(VQ) ->
    case credit_flow:blocked() of
        false -> VQ;
        true  -> receive
                     {bump_credit, Msg} ->
                         credit_flow:handle_bump_msg(Msg),
                         variable_queue_wait_for_shuffling_end(
                           rabbit_variable_queue:resume(VQ))
                 end
    end.

msg2int(#basic_message{content = #content{ payload_fragments_rev = P}}) ->
    binary_to_term(list_to_binary(lists:reverse(P))).

ack_subset(AckSeqs, Interval, Rem) ->
    lists:filter(fun ({_Ack, N}) -> (N + Rem) rem Interval == 0 end, AckSeqs).

requeue_one_by_one(Acks, VQ) ->
    lists:foldl(fun (AckTag, VQN) ->
                        {_MsgId, VQM} = rabbit_variable_queue:requeue(
                                          [AckTag], VQN),
                        VQM
                end, VQ, Acks).

%% Create a vq with messages in q1, delta, and q3, and holes (in the
%% form of pending acks) in the latter two.
variable_queue_with_holes(VQ0) ->
    Interval = 2048, %% should match vq:IO_BATCH_SIZE
    Count = rabbit_queue_index:next_segment_boundary(0)*2 + 2 * Interval,
    Seq = lists:seq(1, Count),
    VQ1 = variable_queue_set_ram_duration_target(0, VQ0),
    VQ2 = variable_queue_publish(
            false, 1, Count,
            fun (_, P) -> P end, fun erlang:term_to_binary/1, VQ1),
    {VQ3, AcksR} = variable_queue_fetch(Count, false, false, Count, VQ2),
    Acks = lists:reverse(AcksR),
    AckSeqs = lists:zip(Acks, Seq),
    [{Subset1, _Seq1}, {Subset2, _Seq2}, {Subset3, Seq3}] =
        [lists:unzip(ack_subset(AckSeqs, Interval, I)) || I <- [0, 1, 2]],
    %% we requeue in three phases in order to exercise requeuing logic
    %% in various vq states
    {_MsgIds, VQ4} = rabbit_variable_queue:requeue(
                       Acks -- (Subset1 ++ Subset2 ++ Subset3), VQ3),
    VQ5 = requeue_one_by_one(Subset1, VQ4),
    %% by now we have some messages (and holes) in delta
    VQ6 = requeue_one_by_one(Subset2, VQ5),
    VQ7 = variable_queue_set_ram_duration_target(infinity, VQ6),
    %% add the q1 tail
    VQ8 = variable_queue_publish(
            true, Count + 1, Interval,
            fun (_, P) -> P end, fun erlang:term_to_binary/1, VQ7),
    %% assertions
    Status = variable_queue_status(VQ8),
    vq_with_holes_assertions(VQ8, proplists:get_value(mode, Status)),
    Depth = Count + Interval,
    Depth = rabbit_variable_queue:depth(VQ8),
    Len = Depth - length(Subset3),
    Len = rabbit_variable_queue:len(VQ8),
    {Seq3, Seq -- Seq3, lists:seq(Count + 1, Count + Interval), VQ8}.

vq_with_holes_assertions(VQ, default) ->
    [false =
         case V of
             {delta, _, 0, _} -> true;
             0                -> true;
             _                -> false
         end || {K, V} <- variable_queue_status(VQ),
                lists:member(K, [q1, delta, q3])];
vq_with_holes_assertions(VQ, lazy) ->
    [false =
         case V of
             {delta, _, 0, _} -> true;
             _                -> false
         end || {K, V} <- variable_queue_status(VQ),
                lists:member(K, [delta])].

check_variable_queue_status(VQ0, Props) ->
    VQ1 = variable_queue_wait_for_shuffling_end(VQ0),
    S = variable_queue_status(VQ1),
    assert_props(S, Props),
    VQ1.

%% ---------------------------------------------------------------------------
%% Credit flow.
%% ---------------------------------------------------------------------------

credit_flow_settings(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, credit_flow_settings1, [Config]).

credit_flow_settings1(_Config) ->
    %% default values
    passed = test_proc(200, 50),

    application:set_env(rabbit, credit_flow_default_credit, {100, 20}),
    passed = test_proc(100, 20),

    application:unset_env(rabbit, credit_flow_default_credit),

    % back to defaults
    passed = test_proc(200, 50),
    passed.

test_proc(InitialCredit, MoreCreditAfter) ->
    Pid = spawn(fun dummy/0),
    Pid ! {credit, self()},
    {InitialCredit, MoreCreditAfter} =
        receive
            {credit, Val} -> Val
        end,
    passed.

dummy() ->
    credit_flow:send(self()),
    receive
        {credit, From} ->
            From ! {credit, get(credit_flow_default_credit)};
        _      ->
            dummy()
    end.

%% ---------------------------------------------------------------------------
%% file_handle_cache.
%% ---------------------------------------------------------------------------

file_handle_cache(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, file_handle_cache1, [Config]).

file_handle_cache1(_Config) ->
    %% test copying when there is just one spare handle
    Limit = file_handle_cache:get_limit(),
    ok = file_handle_cache:set_limit(5), %% 1 or 2 sockets, 2 msg_stores
    TmpDir = filename:join(rabbit_mnesia:dir(), "tmp"),
    ok = filelib:ensure_dir(filename:join(TmpDir, "nothing")),
    [Src1, Dst1, Src2, Dst2] = Files =
        [filename:join(TmpDir, Str) || Str <- ["file1", "file2", "file3", "file4"]],
    Content = <<"foo">>,
    CopyFun = fun (Src, Dst) ->
                      {ok, Hdl} = prim_file:open(Src, [binary, write]),
                      ok = prim_file:write(Hdl, Content),
                      ok = prim_file:sync(Hdl),
                      prim_file:close(Hdl),

                      {ok, SrcHdl} = file_handle_cache:open(Src, [read], []),
                      {ok, DstHdl} = file_handle_cache:open(Dst, [write], []),
                      Size = size(Content),
                      {ok, Size} = file_handle_cache:copy(SrcHdl, DstHdl, Size),
                      ok = file_handle_cache:delete(SrcHdl),
                      ok = file_handle_cache:delete(DstHdl)
              end,
    Pid = spawn(fun () -> {ok, Hdl} = file_handle_cache:open(
                                        filename:join(TmpDir, "file5"),
                                        [write], []),
                          receive {next, Pid1} -> Pid1 ! {next, self()} end,
                          file_handle_cache:delete(Hdl),
                          %% This will block and never return, so we
                          %% exercise the fhc tidying up the pending
                          %% queue on the death of a process.
                          ok = CopyFun(Src1, Dst1)
                end),
    ok = CopyFun(Src1, Dst1),
    ok = file_handle_cache:set_limit(2),
    Pid ! {next, self()},
    receive {next, Pid} -> ok end,
    timer:sleep(100),
    Pid1 = spawn(fun () -> CopyFun(Src2, Dst2) end),
    timer:sleep(100),
    erlang:monitor(process, Pid),
    erlang:monitor(process, Pid1),
    exit(Pid, kill),
    exit(Pid1, kill),
    receive {'DOWN', _MRef, process, Pid, _Reason} -> ok end,
    receive {'DOWN', _MRef1, process, Pid1, _Reason1} -> ok end,
    [file:delete(File) || File <- Files],
    ok = file_handle_cache:set_limit(Limit),
    passed.

%% -------------------------------------------------------------------
%% Log management.
%% -------------------------------------------------------------------

log_management(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, log_management1, [Config]).

log_management1(_Config) ->
    override_group_leader(),

    MainLog = rabbit:log_location(kernel),
    SaslLog = rabbit:log_location(sasl),
    Suffix = ".1",

    ok = test_logs_working(MainLog, SaslLog),

    %% prepare basic logs
    file:delete([MainLog, Suffix]),
    file:delete([SaslLog, Suffix]),

    %% simple logs reopening
    ok = rabbit_ct_helpers:control_action(rotate_logs, []),
    ok = test_logs_working(MainLog, SaslLog),

    %% simple log rotation
    ok = rabbit_ct_helpers:control_action(rotate_logs, [Suffix]),
    [true, true] = non_empty_files([[MainLog, Suffix], [SaslLog, Suffix]]),
    [true, true] = empty_files([MainLog, SaslLog]),
    ok = test_logs_working(MainLog, SaslLog),

    %% reopening logs with log rotation performed first
    ok = clean_logs([MainLog, SaslLog], Suffix),
    ok = rabbit_ct_helpers:control_action(rotate_logs, []),
    ok = file:rename(MainLog, [MainLog, Suffix]),
    ok = file:rename(SaslLog, [SaslLog, Suffix]),
    ok = test_logs_working([MainLog, Suffix], [SaslLog, Suffix]),
    ok = rabbit_ct_helpers:control_action(rotate_logs, []),
    ok = test_logs_working(MainLog, SaslLog),

    %% log rotation on empty files (the main log will have a ctl action logged)
    ok = clean_logs([MainLog, SaslLog], Suffix),
    ok = rabbit_ct_helpers:control_action(rotate_logs, []),
    ok = rabbit_ct_helpers:control_action(rotate_logs, [Suffix]),
    [false, true] = empty_files([[MainLog, Suffix], [SaslLog, Suffix]]),

    %% logs with suffix are not writable
    ok = rabbit_ct_helpers:control_action(rotate_logs, [Suffix]),
    ok = make_files_non_writable([[MainLog, Suffix], [SaslLog, Suffix]]),
    ok = rabbit_ct_helpers:control_action(rotate_logs, [Suffix]),
    ok = test_logs_working(MainLog, SaslLog),

    %% logging directed to tty (first, remove handlers)
    ok = delete_log_handlers([rabbit_sasl_report_file_h,
                              rabbit_error_logger_file_h]),
    ok = clean_logs([MainLog, SaslLog], Suffix),
    ok = application:set_env(rabbit, sasl_error_logger, tty),
    ok = application:set_env(rabbit, error_logger, tty),
    ok = rabbit_ct_helpers:control_action(rotate_logs, []),
    [{error, enoent}, {error, enoent}] = empty_files([MainLog, SaslLog]),

    %% rotate logs when logging is turned off
    ok = application:set_env(rabbit, sasl_error_logger, false),
    ok = application:set_env(rabbit, error_logger, silent),
    ok = rabbit_ct_helpers:control_action(rotate_logs, []),
    [{error, enoent}, {error, enoent}] = empty_files([MainLog, SaslLog]),

    %% cleanup
    ok = application:set_env(rabbit, sasl_error_logger, {file, SaslLog}),
    ok = application:set_env(rabbit, error_logger, {file, MainLog}),
    ok = add_log_handlers([{rabbit_error_logger_file_h, MainLog},
                           {rabbit_sasl_report_file_h, SaslLog}]),
    passed.

log_management_during_startup(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, log_management_during_startup1, [Config]).

log_management_during_startup1(_Config) ->
    MainLog = rabbit:log_location(kernel),
    SaslLog = rabbit:log_location(sasl),

    %% start application with simple tty logging
    ok = rabbit_ct_helpers:control_action(stop_app, []),
    ok = application:set_env(rabbit, error_logger, tty),
    ok = application:set_env(rabbit, sasl_error_logger, tty),
    ok = add_log_handlers([{error_logger_tty_h, []},
                           {sasl_report_tty_h, []}]),
    ok = rabbit_ct_helpers:control_action(start_app, []),

    %% start application with tty logging and
    %% proper handlers not installed
    ok = rabbit_ct_helpers:control_action(stop_app, []),
    ok = error_logger:tty(false),
    ok = delete_log_handlers([sasl_report_tty_h]),
    ok = case catch rabbit_ct_helpers:control_action(start_app, []) of
             ok -> exit({got_success_but_expected_failure,
                         log_rotation_tty_no_handlers_test});
             {badrpc, {'EXIT', {error,
                                {cannot_log_to_tty, _, not_installed}}}} -> ok
         end,

    %% fix sasl logging
    ok = application:set_env(rabbit, sasl_error_logger, {file, SaslLog}),

    %% start application with logging to non-existing directory
    TmpLog = "/tmp/rabbit-tests/test.log",
    delete_file(TmpLog),
    ok = rabbit_ct_helpers:control_action(stop_app, []),
    ok = application:set_env(rabbit, error_logger, {file, TmpLog}),

    ok = delete_log_handlers([rabbit_error_logger_file_h]),
    ok = add_log_handlers([{error_logger_file_h, MainLog}]),
    ok = rabbit_ct_helpers:control_action(start_app, []),

    %% start application with logging to directory with no
    %% write permissions
    ok = rabbit_ct_helpers:control_action(stop_app, []),
    TmpDir = "/tmp/rabbit-tests",
    ok = set_permissions(TmpDir, 8#00400),
    ok = delete_log_handlers([rabbit_error_logger_file_h]),
    ok = add_log_handlers([{error_logger_file_h, MainLog}]),
    ok = case rabbit_ct_helpers:control_action(start_app, []) of
             ok -> exit({got_success_but_expected_failure,
                         log_rotation_no_write_permission_dir_test});
             {badrpc, {'EXIT',
                       {error, {cannot_log_to_file, _, _}}}} -> ok
         end,

    %% start application with logging to a subdirectory which
    %% parent directory has no write permissions
    ok = rabbit_ct_helpers:control_action(stop_app, []),
    TmpTestDir = "/tmp/rabbit-tests/no-permission/test/log",
    ok = application:set_env(rabbit, error_logger, {file, TmpTestDir}),
    ok = add_log_handlers([{error_logger_file_h, MainLog}]),
    ok = case rabbit_ct_helpers:control_action(start_app, []) of
             ok -> exit({got_success_but_expected_failure,
                         log_rotatation_parent_dirs_test});
             {badrpc,
              {'EXIT',
               {error, {cannot_log_to_file, _,
                        {error,
                         {cannot_create_parent_dirs, _, eacces}}}}}} -> ok
         end,
    ok = set_permissions(TmpDir, 8#00700),
    ok = set_permissions(TmpLog, 8#00600),
    ok = delete_file(TmpLog),
    ok = file:del_dir(TmpDir),

    %% start application with standard error_logger_file_h
    %% handler not installed
    ok = rabbit_ct_helpers:control_action(stop_app, []),
    ok = application:set_env(rabbit, error_logger, {file, MainLog}),
    ok = rabbit_ct_helpers:control_action(start_app, []),

    %% start application with standard sasl handler not installed
    %% and rabbit main log handler installed correctly
    ok = rabbit_ct_helpers:control_action(stop_app, []),
    ok = delete_log_handlers([rabbit_sasl_report_file_h]),
    ok = rabbit_ct_helpers:control_action(start_app, []),
    passed.

override_group_leader() ->
    %% Override group leader, otherwise SASL fake events are ignored by
    %% the error_logger local to RabbitMQ.
    {group_leader, Leader} = erlang:process_info(whereis(rabbit), group_leader),
    io:format("Moving group leader to ~p~n", [Leader]),
    erlang:group_leader(Leader, self()).

empty_files(Files) ->
    [case file:read_file_info(File) of
         {ok, FInfo} -> FInfo#file_info.size == 0;
         Error       -> Error
     end || File <- Files].

non_empty_files(Files) ->
    [case EmptyFile of
         {error, Reason} -> {error, Reason};
         _               -> not(EmptyFile)
     end || EmptyFile <- empty_files(Files)].

test_logs_working(MainLogFile, SaslLogFile) ->
    ok = rabbit_log:error("foo bar~n"),
    ok = error_logger:error_report(crash_report, [foo, bar]),
    %% give the error loggers some time to catch up
    timer:sleep(100),
    [true, true] = non_empty_files([MainLogFile, SaslLogFile]),
    ok.

set_permissions(Path, Mode) ->
    case file:read_file_info(Path) of
        {ok, FInfo} -> file:write_file_info(
                         Path,
                         FInfo#file_info{mode=Mode});
        Error       -> Error
    end.

clean_logs(Files, Suffix) ->
    [begin
         ok = delete_file(File),
         ok = delete_file([File, Suffix])
     end || File <- Files],
    ok.

assert_ram_node() ->
    case rabbit_mnesia:node_type() of
        disc -> exit('not_ram_node');
        ram  -> ok
    end.

assert_disc_node() ->
    case rabbit_mnesia:node_type() of
        disc -> ok;
        ram  -> exit('not_disc_node')
    end.

delete_file(File) ->
    case file:delete(File) of
        ok              -> ok;
        {error, enoent} -> ok;
        Error           -> Error
    end.

make_files_non_writable(Files) ->
    [ok = file:write_file_info(File, #file_info{mode=8#444}) ||
        File <- Files],
    ok.

add_log_handlers(Handlers) ->
    [ok = error_logger:add_report_handler(Handler, Args) ||
        {Handler, Args} <- Handlers],
    ok.

%% sasl_report_file_h returns [] during terminate
%% see: https://github.com/erlang/otp/blob/maint/lib/stdlib/src/error_logger_file_h.erl#L98
%%
%% error_logger_file_h returns ok since OTP 18.1
%% see: https://github.com/erlang/otp/blob/maint/lib/stdlib/src/error_logger_file_h.erl#L98
delete_log_handlers(Handlers) ->
    [ok_or_empty_list(error_logger:delete_report_handler(Handler))
     || Handler <- Handlers],
    ok.

ok_or_empty_list([]) ->
    [];
ok_or_empty_list(ok) ->
    ok.

%% ---------------------------------------------------------------------------
%% Password hashing.
%% ---------------------------------------------------------------------------

password_hashing(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, password_hashing1, [Config]).

password_hashing1(_Config) ->
    rabbit_password_hashing_sha256 = rabbit_password:hashing_mod(),
    application:set_env(rabbit, password_hashing_module,
                        rabbit_password_hashing_md5),
    rabbit_password_hashing_md5    = rabbit_password:hashing_mod(),
    application:set_env(rabbit, password_hashing_module,
                        rabbit_password_hashing_sha256),
    rabbit_password_hashing_sha256 = rabbit_password:hashing_mod(),

    rabbit_password_hashing_sha256 =
        rabbit_password:hashing_mod(rabbit_password_hashing_sha256),
    rabbit_password_hashing_md5    =
        rabbit_password:hashing_mod(rabbit_password_hashing_md5),
    rabbit_password_hashing_md5    =
        rabbit_password:hashing_mod(undefined),

    rabbit_password_hashing_md5    =
        rabbit_auth_backend_internal:hashing_module_for_user(
          #internal_user{}),
    rabbit_password_hashing_md5    =
        rabbit_auth_backend_internal:hashing_module_for_user(
          #internal_user{
             hashing_algorithm = undefined
            }),
    rabbit_password_hashing_md5    =
        rabbit_auth_backend_internal:hashing_module_for_user(
          #internal_user{
             hashing_algorithm = rabbit_password_hashing_md5
            }),

    rabbit_password_hashing_sha256 =
        rabbit_auth_backend_internal:hashing_module_for_user(
          #internal_user{
             hashing_algorithm = rabbit_password_hashing_sha256
            }),

    passed.

change_password(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, change_password1, [Config]).

change_password1(_Config) ->
    UserName = <<"test_user">>,
    Password = <<"test_password">>,
    case rabbit_auth_backend_internal:lookup_user(UserName) of
        {ok, _} -> rabbit_auth_backend_internal:delete_user(UserName);
        _       -> ok
    end,
    ok = application:set_env(rabbit, password_hashing_module,
                             rabbit_password_hashing_md5),
    ok = rabbit_auth_backend_internal:add_user(UserName, Password),
    {ok, #auth_user{username = UserName}} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, Password}]),
    ok = application:set_env(rabbit, password_hashing_module,
                             rabbit_password_hashing_sha256),
    {ok, #auth_user{username = UserName}} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, Password}]),

    NewPassword = <<"test_password1">>,
    ok = rabbit_auth_backend_internal:change_password(UserName, NewPassword),
    {ok, #auth_user{username = UserName}} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, NewPassword}]),

    {refused, _, [UserName]} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, Password}]),
    passed.

%% -------------------------------------------------------------------
%% rabbitmqctl.
%% -------------------------------------------------------------------

list_operations_timeout_pass(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, list_operations_timeout_pass1, [Config]).

list_operations_timeout_pass1(_Config) ->
    %% create a few things so there is some useful information to list
    {_Writer1, Limiter1, Ch1} = rabbit_ct_broker_helpers:test_channel(),
    {_Writer2, Limiter2, Ch2} = rabbit_ct_broker_helpers:test_channel(),

    [Q, Q2] = [Queue || Name <- [<<"foo">>, <<"bar">>],
                        {new, Queue = #amqqueue{}} <-
                            [rabbit_amqqueue:declare(
                               rabbit_misc:r(<<"/">>, queue, Name),
                               false, false, [], none)]],

    ok = rabbit_amqqueue:basic_consume(
           Q, true, Ch1, Limiter1, false, 0, <<"ctag1">>, true, [],
           undefined),
    ok = rabbit_amqqueue:basic_consume(
           Q2, true, Ch2, Limiter2, false, 0, <<"ctag2">>, true, [],
           undefined),

    %% list users
    ok = rabbit_ct_helpers:control_action(add_user, ["foo", "bar"]),
    {error, {user_already_exists, _}} =
        rabbit_ct_helpers:control_action(add_user, ["foo", "bar"]),
    ok = rabbit_ct_helpers:control_action_t(list_users, [],
                                            ?TIMEOUT_LIST_OPS_PASS),

    %% list parameters
    ok = rabbit_runtime_parameters_test:register(),
    ok = rabbit_ct_helpers:control_action(set_parameter,
                                          ["test", "good", "123"]),
    ok = rabbit_ct_helpers:control_action_t(list_parameters, [],
                                            ?TIMEOUT_LIST_OPS_PASS),
    ok = rabbit_ct_helpers:control_action(clear_parameter,
                                          ["test", "good"]),
    rabbit_runtime_parameters_test:unregister(),

    %% list vhosts
    ok = rabbit_ct_helpers:control_action(add_vhost, ["/testhost"]),
    {error, {vhost_already_exists, _}} =
        rabbit_ct_helpers:control_action(add_vhost, ["/testhost"]),
    ok = rabbit_ct_helpers:control_action_t(list_vhosts, [],
                                            ?TIMEOUT_LIST_OPS_PASS),

    %% list permissions
    ok = rabbit_ct_helpers:control_action(set_permissions,
                                          ["foo", ".*", ".*", ".*"],
                                          [{"-p", "/testhost"}]),
    ok = rabbit_ct_helpers:control_action_t(list_permissions, [],
                                            [{"-p", "/testhost"}],
                                            ?TIMEOUT_LIST_OPS_PASS),

    %% list user permissions
    ok = rabbit_ct_helpers:control_action_t(list_user_permissions, ["foo"],
                                            ?TIMEOUT_LIST_OPS_PASS),

    %% list policies
    ok = rabbit_ct_helpers:control_action_opts(["set_policy", "name", ".*",
                                                "{\"ha-mode\":\"all\"}"]),
    ok = rabbit_ct_helpers:control_action_t(list_policies, [],
                                            ?TIMEOUT_LIST_OPS_PASS),
    ok = rabbit_ct_helpers:control_action(clear_policy, ["name"]),

    %% list queues
    ok = rabbit_ct_helpers:info_action_t(list_queues,
                                         rabbit_amqqueue:info_keys(), false,
                                         ?TIMEOUT_LIST_OPS_PASS),

    %% list exchanges
    ok = rabbit_ct_helpers:info_action_t(list_exchanges,
                                         rabbit_exchange:info_keys(), true,
                                         ?TIMEOUT_LIST_OPS_PASS),

    %% list bindings
    ok = rabbit_ct_helpers:info_action_t(list_bindings,
                                         rabbit_binding:info_keys(), true,
                                         ?TIMEOUT_LIST_OPS_PASS),

    %% list connections
    {H, P} = rabbit_ct_broker_helpers:find_listener(),
    {ok, C1} = gen_tcp:connect(H, P, [binary, {active, false}]),
    gen_tcp:send(C1, <<"AMQP", 0, 0, 9, 1>>),
    {ok, <<1,0,0>>} = gen_tcp:recv(C1, 3, 100),

    {ok, C2} = gen_tcp:connect(H, P, [binary, {active, false}]),
    gen_tcp:send(C2, <<"AMQP", 0, 0, 9, 1>>),
    {ok, <<1,0,0>>} = gen_tcp:recv(C2, 3, 100),

    ok = rabbit_ct_helpers:info_action_t(
      list_connections, rabbit_networking:connection_info_keys(), false,
      ?TIMEOUT_LIST_OPS_PASS),

    %% list consumers
    ok = rabbit_ct_helpers:info_action_t(
      list_consumers, rabbit_amqqueue:consumer_info_keys(), false,
      ?TIMEOUT_LIST_OPS_PASS),

    %% list channels
    ok = rabbit_ct_helpers:info_action_t(
      list_channels, rabbit_channel:info_keys(), false,
      ?TIMEOUT_LIST_OPS_PASS),

    %% do some cleaning up
    ok = rabbit_ct_helpers:control_action(delete_user, ["foo"]),
    {error, {no_such_user, _}} =
        rabbit_ct_helpers:control_action(delete_user, ["foo"]),

    ok = rabbit_ct_helpers:control_action(delete_vhost, ["/testhost"]),
    {error, {no_such_vhost, _}} =
        rabbit_ct_helpers:control_action(delete_vhost, ["/testhost"]),

    %% close_connection
    Conns = rabbit_networking:connections(),
    [ok = rabbit_ct_helpers:control_action(
        close_connection, [rabbit_misc:pid_to_string(ConnPid), "go away"])
     || ConnPid <- Conns],

    %% cleanup queues
    [{ok, _} = rabbit_amqqueue:delete(QR, false, false) || QR <- [Q, Q2]],

    [begin
         unlink(Chan),
         ok = rabbit_channel:shutdown(Chan)
     end || Chan <- [Ch1, Ch2]],
    passed.

%% -------------------------------------------------------------------
%% Topic matching.
%% -------------------------------------------------------------------

topic_matching(Config) ->
    passed = rabbit_ct_broker_helpers:run_on_broker(
      ?config(rmq_nodename, Config),
      ?MODULE, topic_matching1, [Config]).

topic_matching1(_Config) ->
    XName = #resource{virtual_host = <<"/">>,
                      kind = exchange,
                      name = <<"test_exchange">>},
    X0 = #exchange{name = XName, type = topic, durable = false,
                   auto_delete = false, arguments = []},
    X = rabbit_exchange_decorator:set(X0),
    %% create
    rabbit_exchange_type_topic:validate(X),
    exchange_op_callback(X, create, []),

    %% add some bindings
    Bindings = [#binding{source = XName,
                         key = list_to_binary(Key),
                         destination = #resource{virtual_host = <<"/">>,
                                                 kind = queue,
                                                 name = list_to_binary(Q)},
                         args = Args} ||
                   {Key, Q, Args} <- [{"a.b.c",         "t1",  []},
                                      {"a.*.c",         "t2",  []},
                                      {"a.#.b",         "t3",  []},
                                      {"a.b.b.c",       "t4",  []},
                                      {"#",             "t5",  []},
                                      {"#.#",           "t6",  []},
                                      {"#.b",           "t7",  []},
                                      {"*.*",           "t8",  []},
                                      {"a.*",           "t9",  []},
                                      {"*.b.c",         "t10", []},
                                      {"a.#",           "t11", []},
                                      {"a.#.#",         "t12", []},
                                      {"b.b.c",         "t13", []},
                                      {"a.b.b",         "t14", []},
                                      {"a.b",           "t15", []},
                                      {"b.c",           "t16", []},
                                      {"",              "t17", []},
                                      {"*.*.*",         "t18", []},
                                      {"vodka.martini", "t19", []},
                                      {"a.b.c",         "t20", []},
                                      {"*.#",           "t21", []},
                                      {"#.*.#",         "t22", []},
                                      {"*.#.#",         "t23", []},
                                      {"#.#.#",         "t24", []},
                                      {"*",             "t25", []},
                                      {"#.b.#",         "t26", []},
                                      {"args-test",     "t27",
                                       [{<<"foo">>, longstr, <<"bar">>}]},
                                      {"args-test",     "t27", %% Note aliasing
                                       [{<<"foo">>, longstr, <<"baz">>}]}]],
    lists:foreach(fun (B) -> exchange_op_callback(X, add_binding, [B]) end,
                  Bindings),

    %% test some matches
    test_topic_expect_match(
      X, [{"a.b.c",               ["t1", "t2", "t5", "t6", "t10", "t11", "t12",
                                   "t18", "t20", "t21", "t22", "t23", "t24",
                                   "t26"]},
          {"a.b",                 ["t3", "t5", "t6", "t7", "t8", "t9", "t11",
                                   "t12", "t15", "t21", "t22", "t23", "t24",
                                   "t26"]},
          {"a.b.b",               ["t3", "t5", "t6", "t7", "t11", "t12", "t14",
                                   "t18", "t21", "t22", "t23", "t24", "t26"]},
          {"",                    ["t5", "t6", "t17", "t24"]},
          {"b.c.c",               ["t5", "t6", "t18", "t21", "t22", "t23",
                                   "t24", "t26"]},
          {"a.a.a.a.a",           ["t5", "t6", "t11", "t12", "t21", "t22",
                                   "t23", "t24"]},
          {"vodka.gin",           ["t5", "t6", "t8", "t21", "t22", "t23",
                                   "t24"]},
          {"vodka.martini",       ["t5", "t6", "t8", "t19", "t21", "t22", "t23",
                                   "t24"]},
          {"b.b.c",               ["t5", "t6", "t10", "t13", "t18", "t21",
                                   "t22", "t23", "t24", "t26"]},
          {"nothing.here.at.all", ["t5", "t6", "t21", "t22", "t23", "t24"]},
          {"oneword",             ["t5", "t6", "t21", "t22", "t23", "t24",
                                   "t25"]},
          {"args-test",           ["t5", "t6", "t21", "t22", "t23", "t24",
                                   "t25", "t27"]}]),
    %% remove some bindings
    RemovedBindings = [lists:nth(1, Bindings), lists:nth(5, Bindings),
                       lists:nth(11, Bindings), lists:nth(19, Bindings),
                       lists:nth(21, Bindings), lists:nth(28, Bindings)],
    exchange_op_callback(X, remove_bindings, [RemovedBindings]),
    RemainingBindings = ordsets:to_list(
                          ordsets:subtract(ordsets:from_list(Bindings),
                                           ordsets:from_list(RemovedBindings))),

    %% test some matches
    test_topic_expect_match(
      X,
      [{"a.b.c",               ["t2", "t6", "t10", "t12", "t18", "t20", "t22",
                                "t23", "t24", "t26"]},
       {"a.b",                 ["t3", "t6", "t7", "t8", "t9", "t12", "t15",
                                "t22", "t23", "t24", "t26"]},
       {"a.b.b",               ["t3", "t6", "t7", "t12", "t14", "t18", "t22",
                                "t23", "t24", "t26"]},
       {"",                    ["t6", "t17", "t24"]},
       {"b.c.c",               ["t6", "t18", "t22", "t23", "t24", "t26"]},
       {"a.a.a.a.a",           ["t6", "t12", "t22", "t23", "t24"]},
       {"vodka.gin",           ["t6", "t8", "t22", "t23", "t24"]},
       {"vodka.martini",       ["t6", "t8", "t22", "t23", "t24"]},
       {"b.b.c",               ["t6", "t10", "t13", "t18", "t22", "t23",
                                "t24", "t26"]},
       {"nothing.here.at.all", ["t6", "t22", "t23", "t24"]},
       {"oneword",             ["t6", "t22", "t23", "t24", "t25"]},
       {"args-test",           ["t6", "t22", "t23", "t24", "t25", "t27"]}]),

    %% remove the entire exchange
    exchange_op_callback(X, delete, [RemainingBindings]),
    %% none should match now
    test_topic_expect_match(X, [{"a.b.c", []}, {"b.b.c", []}, {"", []}]),
    passed.

exchange_op_callback(X, Fun, Args) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () -> rabbit_exchange:callback(X, Fun, transaction, [X] ++ Args) end),
    rabbit_exchange:callback(X, Fun, none, [X] ++ Args).

test_topic_expect_match(X, List) ->
    lists:foreach(
      fun ({Key, Expected}) ->
              BinKey = list_to_binary(Key),
              Message = rabbit_basic:message(X#exchange.name, BinKey,
                                             #'P_basic'{}, <<>>),
              Res = rabbit_exchange_type_topic:route(
                      X, #delivery{mandatory = false,
                                   sender    = self(),
                                   message   = Message}),
              ExpectedRes = lists:map(
                              fun (Q) -> #resource{virtual_host = <<"/">>,
                                                   kind = queue,
                                                   name = list_to_binary(Q)}
                              end, Expected),
              true = (lists:usort(ExpectedRes) =:= lists:usort(Res))
      end, List).
