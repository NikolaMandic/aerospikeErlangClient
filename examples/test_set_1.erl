%%% Copyright (C) 2013  by Aerospike.  All rights reserved.  
%%% THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
%%%  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.        
%%
%% This benchmark exercises Aerospike's Erlang Client API to
%% perform a variable benchmark, where the following can be varied:
%% + Number of Processes
%% + Number of Operations per process
%% + Read/Write distribution (relative percentages)
%% + Key Value Range
%% + Key Size
%% + Value Size
%% 
%% 1/10/2013 tjl:
%% Define this module:  asbench.erl
-module(test_set_1).

%% Import the Aerospike API module and Define those functions from it that 
%% we will (or may) use in this module
%% -import(aerospike,
%% [connect/2, put/6, get/6, getAll/5, histogram/1, stopwatch/1]).

%% Import the Aerospike Benchmark module that defines the actual benchmark
%% function -- that spawns the processes that fire off the test workers.
-import(asbench,
  [bench/11, doOp/11]).

%% Import the Helper functions to manage Read/Write Stats.
% -import(stats_record,
%   [makeRecord/0, incrReadOk/1, incrReadErr/1, incrWriteOk/1, incrWriteErr/1]).

%% Expose the following to be called either by other modules or by the
%% client shell (command line).
-export([
    ts1_test0/0, %% Preset Benchmark Test
    ts1_test00/0, %% Preset Benchmark Test
    ts1_test1/0, %% Preset Benchmark Test
    ts1_test2/0,%% Preset Benchmark Test
    ts1_test21/0,%% Preset Benchmark Test
    ts1_test3/0,%% Preset Benchmark Test
    ts1_test4/0,%% Preset Benchmark Test
    ts1_test5/0,%% Preset Benchmark Test
    ts1_test0_able/0, %% Preset Benchmark Test:  On remote machine "able"
    ts1_test1_able/0, %% Preset Benchmark Test:  On remote machine "able"
    ts1_test2_able/0, %% Preset Benchmark Test:  On remote machine "able"
    ts1_test3_able/0, %% Preset Benchmark Test:  On remote machine "able"
    ts1_test4_able/0, %% Preset Benchmark Test:  On remote machine "able"
    ts1_test5_able/0  %% Preset Benchmark Test:  On remote machine "able"
  ]).

%% Include our benchmark modules and structures
% -include("asbench.hrl").  %% The stats record
% -include("asbench.erl").  %% The benchmark routines

%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% asbench:bench documentation:
%% Parameters:
%% - Host: The Aerospike Host.  If local host, then use 127.0.0.1
%% - Port: Usually 3000
%% - Namespace: Usually "test"
%% - Set: Often "" or "test_set"
%% - WriteWeight: Distribution of values for Write
%% - ReadWeight: Distribution of values for Read
%% - NumProc: Number of processes to spawn to do the work
%% - NumOp: Number of Operations to perform (either get() or put() ).
%% - KeyUpperRange: The Highest Key value: Key Range:[0 .. KeyUpperRange]
%% - KeySize: The size of a key, in bytes.
%% - ValueSize: The size of a value, in bytes
%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Here are Tests that we can just invoke once from the command line.
%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% ------------------------------------------------------------------------
%% Test 0: Small keys/values,  few Ops/Procs
%% ------------------------------------------------------------------------
ts1_test0() ->
  %% Set the parms
  TestNumber = 0,
  Host = "127.0.0.1",
  Port = 3000,
  Namespace = "test",
  Set = "test_set",
  WriteWeight = 50,
  ReadWeight = 50,
  NumProc = 2,
  NumOps = 10,
  KeyUpperRange = 200,
  KeySize = 32,
  ValueSize = 128,
  io:format("~n<><><> Canned Test (~w) <><><>~n",[TestNumber]),
  io:format("::Host(~s) Port(~w) NameSpace(~s) Set(~s) WriteWeight(~w) ~n",
    [Host, Port, Namespace, Set, WriteWeight] ),
  io:format("::ReadWeight(~w) NumProcs(~w) NumOps(~w) KeyUpperRange(~w) ~n",
    [ReadWeight, NumProc, NumOps, KeyUpperRange]),
  io:format("::KeySize(~w) ValueSize(~w) ~n", [KeySize, ValueSize]),

  %% Do the deed
  asbench:bench(Host, Port, Namespace, Set, WriteWeight, ReadWeight, NumProc,
    NumOps, KeyUpperRange, KeySize, ValueSize ).


%% ------------------------------------------------------------------------
%% Test 00: Small keys/values,  few Ops/Procs
%% ------------------------------------------------------------------------
ts1_test00() ->
  %% Set the parms
  TestNumber = 0,
  Host = "127.0.0.1",
  Port = 3000,
  Namespace = "test",
  Set = "test_set",
  WriteWeight = 50,
  ReadWeight = 50,
  NumProc = 2,
  NumOps = 10,
  KeyUpperRange = 200,
  KeySize = 1,  %% Just use the numbers
  ValueSize = 1,  %% Just use the numbers
  io:format("~n<><><> Canned Test (~w) <><><>~n",[TestNumber]),
  io:format("::Host(~s) Port(~w) NameSpace(~s) Set(~s) WriteWeight(~w) ~n",
    [Host, Port, Namespace, Set, WriteWeight] ),
  io:format("::ReadWeight(~w) NumProcs(~w) NumOps(~w) KeyUpperRange(~w) ~n",
    [ReadWeight, NumProc, NumOps, KeyUpperRange]),
  io:format("::KeySize(~w) ValueSize(~w) ~n", [KeySize, ValueSize]),

  %% Do the deed
  asbench:bench(Host, Port, Namespace, Set, WriteWeight, ReadWeight, NumProc,
    NumOps, KeyUpperRange, KeySize, ValueSize ).

%% ------------------------------------------------------------------------
%% Test 1:  Small Keys(32)/Values(128) Medium Ops(2000), Procs(16)
%% ------------------------------------------------------------------------
ts1_test1() ->
  %% Set the parms
  TestNumber = 1,
  Host = "127.0.0.1",
  Port = 3000,
  Namespace = "test",
  Set = "test_set",
  WriteWeight = 70,
  ReadWeight = 30,
  NumProc = 16,
  NumOps = 2000,
  KeyUpperRange = 200,
  KeySize = 32,
  ValueSize = 128,
  io:format("~n<><><> Canned Test (~w) <><><>~n",[TestNumber]),
  io:format("::Host(~s) Port(~w) NameSpace(~s) Set(~s) WriteWeight(~w) ~n",
    [Host, Port, Namespace, Set, WriteWeight] ),
  io:format("::ReadWeight(~w) NumProcs(~w) NumOps(~w) KeyUpperRange(~w) ~n",
    [ReadWeight, NumProc, NumOps, KeyUpperRange]),
  io:format("::KeySize(~w) ValueSize(~w) ~n", [KeySize, ValueSize]),

  %% Do the deed
  asbench:bench(Host, Port, Namespace, Set, WriteWeight, ReadWeight, NumProc,
    NumOps, KeyUpperRange, KeySize, ValueSize ).

%% ------------------------------------------------------------------------
%% Test 2:  Small Keys(32)/Values(128) Medium Ops(20,000), Procs(32)
%% ------------------------------------------------------------------------
ts1_test2() ->
  %% Set the parms
  TestNumber = 2,
  Host = "127.0.0.1",
  Port = 3000,
  Namespace = "test",
  Set = "test_set",
  WriteWeight = 30,
  ReadWeight = 70,
  NumProc = 32,
  NumOps = 20000,
  KeyUpperRange = 2000,
  KeySize = 32,
  ValueSize = 128,
  io:format("~n<><><> Canned Test (~w) <><><>~n",[TestNumber]),
  io:format("::Host(~s) Port(~w) NameSpace(~s) Set(~s) WriteWeight(~w) ~n",
    [Host, Port, Namespace, Set, WriteWeight] ),
  io:format("::ReadWeight(~w) NumProcs(~w) NumOps(~w) KeyUpperRange(~w) ~n",
    [ReadWeight, NumProc, NumOps, KeyUpperRange]),
  io:format("::KeySize(~w) ValueSize(~w) ~n", [KeySize, ValueSize]),

  %% Do the deed (dirty deeds, done dirt cheap).
  asbench:bench(Host, Port, Namespace, Set, WriteWeight, ReadWeight, NumProc,
    NumOps, KeyUpperRange, KeySize, ValueSize ).

%% ------------------------------------------------------------------------
%% Test 2.1:  Small Keys(32)/Values(128) Large Ops(100,000), Procs(32)
%% ------------------------------------------------------------------------
ts1_test21() ->
  %% Set the parms
  TestNumber = 2,
  Host = "127.0.0.1",
  Port = 3000,
  Namespace = "test",
  Set = "test_set",
  WriteWeight = 30,
  ReadWeight = 70,
  NumProc = 32,
  NumOps = 100000,
  KeyUpperRange = 10000,
  KeySize = 32,
  ValueSize = 128,
  io:format("~n<><><> Canned Test (~w) <><><>~n",[TestNumber]),
  io:format("::Host(~s) Port(~w) NameSpace(~s) Set(~s) WriteWeight(~w) ~n",
    [Host, Port, Namespace, Set, WriteWeight] ),
  io:format("::ReadWeight(~w) NumProcs(~w) NumOps(~w) KeyUpperRange(~w) ~n",
    [ReadWeight, NumProc, NumOps, KeyUpperRange]),
  io:format("::KeySize(~w) ValueSize(~w) ~n", [KeySize, ValueSize]),

  %% Do the deed (dirty deeds, done dirt cheap).
  asbench:bench(Host, Port, Namespace, Set, WriteWeight, ReadWeight, NumProc,
    NumOps, KeyUpperRange, KeySize, ValueSize ).

%% ------------------------------------------------------------------------
%% Test 3:  Medium Keys(32)/Values(256) Medium Ops(20,000), Procs(32)
%% ------------------------------------------------------------------------
ts1_test3() ->
  %% Set the parms
  TestNumber = 2,
  Host = "127.0.0.1",
  Port = 3000,
  Namespace = "test",
  Set = "test_set",
  WriteWeight = 30,
  ReadWeight = 70,
  NumProc = 32,
  NumOps = 20000,
  KeyUpperRange = 5000,
  KeySize = 32,
  ValueSize = 256,
  io:format("~n<><><> Canned Test (~w) <><><>~n",[TestNumber]),
  io:format("::Host(~s) Port(~w) NameSpace(~s) Set(~s) WriteWeight(~w) ~n",
    [Host, Port, Namespace, Set, WriteWeight] ),
  io:format("::ReadWeight(~w) NumProcs(~w) NumOps(~w) KeyUpperRange(~w) ~n",
    [ReadWeight, NumProc, NumOps, KeyUpperRange]),
  io:format("::KeySize(~w) ValueSize(~w) ~n", [KeySize, ValueSize]),

  %% Do the deed (dirty deeds, done dirt cheap).
  asbench:bench(Host, Port, Namespace, Set, WriteWeight, ReadWeight, NumProc,
    NumOps, KeyUpperRange, KeySize, ValueSize ).

%% ------------------------------------------------------------------------
%% Test 4:  Medium Keys(128)/Values(4096) Medium Ops(20,000), Procs(32)
%% ------------------------------------------------------------------------
ts1_test4() ->
  %% Set the parms
  TestNumber = 2,
  Host = "127.0.0.1",
  Port = 3000,
  Namespace = "test",
  Set = "test_set",
  WriteWeight = 20,
  ReadWeight = 80,
  NumProc = 32,
  NumOps = 20000,
  KeyUpperRange = 2000,
  KeySize = 64,
  ValueSize = 4096,
  io:format("~n<><><> Canned Test (~w) <><><>~n",[TestNumber]),
  io:format("::Host(~s) Port(~w) NameSpace(~s) Set(~s) WriteWeight(~w) ~n",
    [Host, Port, Namespace, Set, WriteWeight] ),
  io:format("::ReadWeight(~w) NumProcs(~w) NumOps(~w) KeyUpperRange(~w) ~n",
    [ReadWeight, NumProc, NumOps, KeyUpperRange]),
  io:format("::KeySize(~w) ValueSize(~w) ~n", [KeySize, ValueSize]),

  %% Do the deed (dirty deeds, done dirt cheap).
  asbench:bench(Host, Port, Namespace, Set, WriteWeight, ReadWeight, NumProc,
    NumOps, KeyUpperRange, KeySize, ValueSize ).

%% ------------------------------------------------------------------------
%% Test 5:  Medium Keys(128)/Values(4096) Medium Ops(20,000), Procs(64)
%% ------------------------------------------------------------------------
ts1_test5() ->
  %% Set the parms
  TestNumber = 2,
  Host = "127.0.0.1",
  Port = 3000,
  Namespace = "test",
  Set = "test_set",
  WriteWeight = 20,
  ReadWeight = 80,
  NumProc = 64,
  NumOps = 20000,
  KeyUpperRange = 20000,
  KeySize = 128,
  ValueSize = 4096,
  io:format("~n<><><> Canned Test (~w) <><><>~n",[TestNumber]),
  io:format("::Host(~s) Port(~w) NameSpace(~s) Set(~s) WriteWeight(~w) ~n",
    [Host, Port, Namespace, Set, WriteWeight] ),
  io:format("::ReadWeight(~w) NumProcs(~w) NumOps(~w) KeyUpperRange(~w) ~n",
    [ReadWeight, NumProc, NumOps, KeyUpperRange]),
  io:format("::KeySize(~w) ValueSize(~w) ~n", [KeySize, ValueSize]),

  %% Do the deed (dirty deeds, done dirt cheap).
  asbench:bench(Host, Port, Namespace, Set, WriteWeight, ReadWeight, NumProc,
    NumOps, KeyUpperRange, KeySize, ValueSize ).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Do some tests on remote machines: able, baker
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% ------------------------------------------------------------------------
%% Test 0: Small keys/values,  few Ops/Procs
%% ------------------------------------------------------------------------
ts1_test0_able() ->
  %% Set the parms
  TestNumber = 0,
  Host = "able",
  Port = 3000,
  Namespace = "test",
  Set = "test_set",
  WriteWeight = 50,
  ReadWeight = 50,
  NumProc = 2,
  NumOps = 10,
  KeyUpperRange = 200,
  KeySize = 32,
  ValueSize = 128,
  io:format("~n<><><> Canned Test (~w) <><><>~n",[TestNumber]),
  io:format("::Host(~s) Port(~w) NameSpace(~s) Set(~s) WriteWeight(~w) ~n",
    [Host, Port, Namespace, Set, WriteWeight] ),
  io:format("::ReadWeight(~w) NumProcs(~w) NumOps(~w) KeyUpperRange(~w) ~n",
    [ReadWeight, NumProc, NumOps, KeyUpperRange]),
  io:format("::KeySize(~w) ValueSize(~w) ~n", [KeySize, ValueSize]),

  %% Do the deed
  asbench:bench(Host, Port, Namespace, Set, WriteWeight, ReadWeight, NumProc,
    NumOps, KeyUpperRange, KeySize, ValueSize ).


%% ------------------------------------------------------------------------
%% Test 1:  Small Keys(32)/Values(128) Medium Ops(2000), Procs(16)
%% ------------------------------------------------------------------------
ts1_test1_able() ->
  %% Set the parms
  TestNumber = 1,
  Host = "able",
  Port = 3000,
  Namespace = "test",
  Set = "test_set",
  WriteWeight = 70,
  ReadWeight = 30,
  NumProc = 16,
  NumOps = 2000,
  KeyUpperRange = 200,
  KeySize = 32,
  ValueSize = 128,
  io:format("~n<><><> Canned Test (~w) <><><>~n",[TestNumber]),
  io:format("::Host(~s) Port(~w) NameSpace(~s) Set(~s) WriteWeight(~w) ~n",
    [Host, Port, Namespace, Set, WriteWeight] ),
  io:format("::ReadWeight(~w) NumProcs(~w) NumOps(~w) KeyUpperRange(~w) ~n",
    [ReadWeight, NumProc, NumOps, KeyUpperRange]),
  io:format("::KeySize(~w) ValueSize(~w) ~n", [KeySize, ValueSize]),

  %% Do the deed
  asbench:bench(Host, Port, Namespace, Set, WriteWeight, ReadWeight, NumProc,
    NumOps, KeyUpperRange, KeySize, ValueSize ).

%% ------------------------------------------------------------------------
%% Test 2:  Small Keys(32)/Values(128) Medium Ops(20,000), Procs(32)
%% ------------------------------------------------------------------------
ts1_test2_able() ->
  %% Set the parms
  TestNumber = 2,
  Host = "able",
  Port = 3000,
  Namespace = "test",
  Set = "test_set",
  WriteWeight = 30,
  ReadWeight = 70,
  NumProc = 32,
  NumOps = 20000,
  KeyUpperRange = 2000,
  KeySize = 32,
  ValueSize = 128,
  io:format("~n<><><> Canned Test (~w) <><><>~n",[TestNumber]),
  io:format("::Host(~s) Port(~w) NameSpace(~s) Set(~s) WriteWeight(~w) ~n",
    [Host, Port, Namespace, Set, WriteWeight] ),
  io:format("::ReadWeight(~w) NumProcs(~w) NumOps(~w) KeyUpperRange(~w) ~n",
    [ReadWeight, NumProc, NumOps, KeyUpperRange]),
  io:format("::KeySize(~w) ValueSize(~w) ~n", [KeySize, ValueSize]),

  %% Do the deed (dirty deeds, done dirt cheap).
  asbench:bench(Host, Port, Namespace, Set, WriteWeight, ReadWeight, NumProc,
    NumOps, KeyUpperRange, KeySize, ValueSize ).

%% ------------------------------------------------------------------------
%% Test 3:  Medium Keys(32)/Values(256) Medium Ops(20,000), Procs(32)
%% ------------------------------------------------------------------------
ts1_test3_able() ->
  %% Set the parms
  TestNumber = 2,
  Host = "able",
  Port = 3000,
  Namespace = "test",
  Set = "test_set",
  WriteWeight = 30,
  ReadWeight = 70,
  NumProc = 32,
  NumOps = 20000,
  KeyUpperRange = 5000,
  KeySize = 32,
  ValueSize = 256,
  io:format("~n<><><> Canned Test (~w) <><><>~n",[TestNumber]),
  io:format("::Host(~s) Port(~w) NameSpace(~s) Set(~s) WriteWeight(~w) ~n",
    [Host, Port, Namespace, Set, WriteWeight] ),
  io:format("::ReadWeight(~w) NumProcs(~w) NumOps(~w) KeyUpperRange(~w) ~n",
    [ReadWeight, NumProc, NumOps, KeyUpperRange]),
  io:format("::KeySize(~w) ValueSize(~w) ~n", [KeySize, ValueSize]),

  %% Do the deed (dirty deeds, done dirt cheap).
  asbench:bench(Host, Port, Namespace, Set, WriteWeight, ReadWeight, NumProc,
    NumOps, KeyUpperRange, KeySize, ValueSize ).

%% ------------------------------------------------------------------------
%% Test 4:  Medium Keys(128)/Values(4096) Medium Ops(20,000), Procs(32)
%% ------------------------------------------------------------------------
ts1_test4_able() ->
  %% Set the parms
  TestNumber = 2,
  Host = "able",
  Port = 3000,
  Namespace = "test",
  Set = "test_set",
  WriteWeight = 20,
  ReadWeight = 80,
  NumProc = 32,
  NumOps = 20000,
  KeyUpperRange = 2000,
  KeySize = 64,
  ValueSize = 4096,
  io:format("~n<><><> Canned Test (~w) <><><>~n",[TestNumber]),
  io:format("::Host(~s) Port(~w) NameSpace(~s) Set(~s) WriteWeight(~w) ~n",
    [Host, Port, Namespace, Set, WriteWeight] ),
  io:format("::ReadWeight(~w) NumProcs(~w) NumOps(~w) KeyUpperRange(~w) ~n",
    [ReadWeight, NumProc, NumOps, KeyUpperRange]),
  io:format("::KeySize(~w) ValueSize(~w) ~n", [KeySize, ValueSize]),

  %% Do the deed (dirty deeds, done dirt cheap).
  asbench:bench(Host, Port, Namespace, Set, WriteWeight, ReadWeight, NumProc,
    NumOps, KeyUpperRange, KeySize, ValueSize ).

%% ------------------------------------------------------------------------
%% Test 5:  Medium Keys(128)/Values(4096) Medium Ops(20,000), Procs(64)
%% ------------------------------------------------------------------------
ts1_test5_able() ->
  %% Set the parms
  TestNumber = 2,
  Host = "able",
  Port = 3000,
  Namespace = "test",
  Set = "test_set",
  WriteWeight = 20,
  ReadWeight = 80,
  NumProc = 64,
  NumOps = 20000,
  KeyUpperRange = 20000,
  KeySize = 128,
  ValueSize = 4096,
  io:format("~n<><><> Canned Test (~w) <><><>~n",[TestNumber]),
  io:format("::Host(~s) Port(~w) NameSpace(~s) Set(~s) WriteWeight(~w) ~n",
    [Host, Port, Namespace, Set, WriteWeight] ),
  io:format("::ReadWeight(~w) NumProcs(~w) NumOps(~w) KeyUpperRange(~w) ~n",
    [ReadWeight, NumProc, NumOps, KeyUpperRange]),
  io:format("::KeySize(~w) ValueSize(~w) ~n", [KeySize, ValueSize]),

  %% Do the deed (dirty deeds, done dirt cheap).
  asbench:bench(Host, Port, Namespace, Set, WriteWeight, ReadWeight, NumProc,
    NumOps, KeyUpperRange, KeySize, ValueSize ).


%% <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF>
