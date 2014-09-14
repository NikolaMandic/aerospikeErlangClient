%%% Copyright (C) 2013  by Aerospike.  All rights reserved.  
%%% THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
%%%  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.        
%%
%% This module contains an example program (called "bench") that performs
%% a (configurable) number operations on the Aerospike Erlang
%% Client.  It's purpose is to demonstrate both the AS Erlang Client API
%% and the performance of the Erlang Client (there are timing functions
%% contained in this module).
%% The "bench()" function allows the caller to specify many parameters:
%% + Number of Processes
%% + Number of Operations per process
%% + Read/Write distribution (relative percentages)
%% + Key Value Range
%% + Key Size
%% + Value Size
%% 

%% Define this module:  asbench.erl
-module(asbench).

%% Include the Aerospike structures
-include("aerospike.hrl").

%% Expose the following to be called either by other modules or by the
%% client shell (command line).
-export([
    bench/11, %% The main workhorse of this test
    doOp/11,  %% Do a specific operation
    test0/0,  %% Simple test to exercise the function
    test1/0   %% Another Simple test to exercise the function
  ]).

%% Import the Aerospike API module and Define those functions from it that 
%% we will use in this module
-import(aerospike,
  [connect/2, put/6, get/6, getAll/5, histogram/1, stopwatch/1]).

%% Import the Helper functions to manage Read/Write Stats.
-import(stats_record,
  [makeRecord/0, incrGeneralErr/1,
    incrReadOk/1, incrReadErr/1, incrWriteOk/1, incrWriteErr/1]).

%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Invoke a benchmark:
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
bench(_Host, _Port, _Ns, _Set, _WriteWeight, _ReadWeight, _NumProc,
      _NumOp, _KeyUpperRange, _KeySize, _ValueSize ) ->
    TotalWeight = _WriteWeight + _ReadWeight,
    {ok, C} = connect(_Host, _Port),
    aerospike:histogram(start),
    aerospike:stopwatch(start),
    io:format("~nBENCHMARK::Timenow(~w) (WriteWeight(~w) ReadWeight(~w)~n",
      [now(), _WriteWeight, _ReadWeight]),
    io:format("::(Procs#(~w) Ops per proc(~w) KeySize(~w) ValueSize(~w) )~n",
          [_NumProc,_NumOp,_KeySize,_ValueSize]),

    spawnProc( C, _Ns, _Set, _WriteWeight, TotalWeight, _NumProc,
              _NumOp, _KeyUpperRange, _KeySize, _ValueSize ).


%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Utility Functions
%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% --------------------------
%% Generate Key
%% --------------------------
%% Generate a Key that is within the key range and is close to "KeySize"
%% Use a Number + "<Key>" as the root and then pad with extra spaces as
%% needed.
generateKey( _KeyUpperRange, _KeySize ) ->
  KeyRoot = random:uniform( _KeyUpperRange ),
  KeyPad = "<Key>",
  KeyRootString = lists:flatten( io_lib:fwrite("~w ~s",[KeyRoot,KeyPad])),
  %Return the results of the "create string" function
  createMyString( KeyRootString, _KeySize ).

%% --------------------------
%% createMyString()
%% --------------------------
%% Recursively build the string until it is greater than or equal to MaxSize
%% This function -- the size requirement is satisfied, so flatten the
%% assembled string and return it.
createMyString( _CurrentString, _MaxSize )
  when length( _CurrentString ) >= _MaxSize ->
    %% The results must be flattened into a pure string (AS CL Requirement)
    lists:flatten( _CurrentString );

%% This function -- the string is not (yet) large enough, so we'll try on
%% some different size padding strings to see which one gets us closest
createMyString( _CurrentString, _MaxSize )
  when length( _CurrentString ) < _MaxSize ->
    % 10 byte padding string
    Pad_10 = "<<PADDING>>",
    % 20 byte padding string
    Pad_20   = "|-- <<PADDING>>-20-|",
    % 100 byte padding string
    Pad_100 = "|-- <<PADDING>>-20-||-- <<PADDING>>-40-||-- <<PADDING>>-60-|"++
              "|-- <<PADDING>>-80-||-- <<PADDING>>100-|",
    % 1,000 byte value (built at compile time)
    Pad_1000= "|-- <<PADDING>>-20-||-- <<PADDING>>-40-||-- <<PADDING>>-60-|"++
              "|-- <<PADDING>>-80-||-- <<PADDING>>100-|"++
              "|-- <<PADDING>>-20-||-- <<PADDING>>-40-||-- <<PADDING>>-60-|"++
              "|-- <<PADDING>>-80-||-- <<PADDING>>200-|"++
              "|-- <<PADDING>>-20-||-- <<PADDING>>-40-||-- <<PADDING>>-60-|"++
              "|-- <<PADDING>>-80-||-- <<PADDING>>300-|"++
              "|-- <<PADDING>>-20-||-- <<PADDING>>-40-||-- <<PADDING>>-60-|"++
              "|-- <<PADDING>>-80-||-- <<PADDING>>400-|"++
              "|-- <<PADDING>>-20-||-- <<PADDING>>-40-||-- <<PADDING>>-60-|"++
              "|-- <<PADDING>>-80-||-- <<PADDING>>500-|"++
              "|-- <<PADDING>>-20-||-- <<PADDING>>-40-||-- <<PADDING>>-60-|"++
              "|-- <<PADDING>>-80-||-- <<PADDING>>600-|"++
              "|-- <<PADDING>>-20-||-- <<PADDING>>-40-||-- <<PADDING>>-60-|"++
              "|-- <<PADDING>>-80-||-- <<PADDING>>700-|"++
              "|-- <<PADDING>>-20-||-- <<PADDING>>-40-||-- <<PADDING>>-60-|"++
              "|-- <<PADDING>>-80-||-- <<PADDING>>800-|"++
              "|-- <<PADDING>>-20-||-- <<PADDING>>-40-||-- <<PADDING>>-60-|"++
              "|-- <<PADDING>>-80-||-- <<PADDING>>900-|"++
              "|-- <<PADDING>>-20-||-- <<PADDING>>-40-||-- <<PADDING>>-60-|"++
              "|-- <<PADDING>>-80-||-- <<PADDING>>1000|",

    Diff = _MaxSize - length( _CurrentString ),
    %% Use the largest possible string to do the job.
    if 
      Diff >= length( Pad_1000)  ->
        NewPadString = Pad_1000;
      Diff >= length( Pad_100)  ->
        NewPadString = Pad_100;
      Diff >= length( Pad_20)  ->
        NewPadString = Pad_20;
      true -> 
        NewPadString = Pad_10
    end,

%%  io:format("Using Pad size(~p) for Diff(~p)~n",
%%    [length(NewPadString), Diff] ),
    createMyString( string:concat( _CurrentString, NewPadString  ), _MaxSize ).

%% --------------------------
%% Generate Value
%% --------------------------
generateValue( _Lottery, _ValueSize ) ->
  ValueRoot = random:uniform( _Lottery ),
  ValuePad = "<-Value-> ",
  ValueString = lists:flatten( io_lib:fwrite("~w ~s",[ValueRoot,ValuePad])),
  createMyString( ValueString, _ValueSize ).

%% ---------------------------------------------
%% Process the results of a aerospike Function
%% We process these from MOST SPECIFIC to LEAST SPECIFIC
%% ---------------------------------------------
%% Start with the most specific, selective result:  Write OK
processResults( ok, write, _StatsRecord ) ->
% io:format("[~s]:Write OK: Incr W OK!! (~p) ~n ",["ProcessResults", write ]),
  stats_record:incrWriteOk( _StatsRecord );

%% Start with the most specific, selective result:  Write OK
processResults( {error, _Msg }, write, _StatsRecord ) ->
% io:format("[~s]:WRITE ERROR: Msg(~p) ~n ",["ProcessResults", _Msg]),
  stats_record:incrWriteErr( _StatsRecord );

%% All Reads come back with either
%% Result Tuple: {{bin1, val1}, {bin2, Val2} ... {binN, ValN}}
%% {  aerospike_error , { Number, Msg } }
%% Not sure if we'll ever see an error without a message.
processResults( { aerospike_error, _ErrorList},  read, _StatsRecord ) ->
% io:format("[~s]:Read ERROR: Inc R Err!! ~n ",["ProcessResults"]),
  stats_record:incrReadErr( _StatsRecord );

processResults( [_GoodResult], read, _StatsRecord) ->
%   io:format("[~s]: Read Good: Results(~p) SR(~p)  ~n ",
%     ["ProcessResults",  _GoodResult,  _StatsRecord ]),
  stats_record:incrReadOk( _StatsRecord );

processResults( ok, _Op, _StatsRecord ) ->
% io:format("[~s]:Unknown Op(~p)  (but OK)  ~n ",["ProcessResults",_Op ]),
  stats_record:incrGeneralOk( _StatsRecord );

processResults({ok, _Results}, _Op, _StatsRecord ) ->
% io:format("[~s]:Unknown Op(~p)  (but OK): Results(~p) ~n ",
%   ["ProcessResults",_Op, _Results ]),
  stats_record:incrGeneralOk( _StatsRecord );

processResults( aerospike_error, _Op, _StatsRecord ) ->
% io:format("[~s]:General ERROR(~p) Op(~p) ~n ",
%   ["ProcessResults", aerospike_error, _Op ]),
  stats_record:incrGeneralErr( _StatsRecord );

processResults( error, _Op, _StatsRecord ) ->
% io:format("[~s]:General ERROR(~p) Op(~p) ~n ",
%   ["ProcessResults", error, _Op ]),
  stats_record:incrGeneralErr( _StatsRecord );

%% This is the most general case -- the "Catch All" case.
%% IN theory we should never land here -- if we are all consistent.
processResults( _Results, _Op, _StatsRecord ) ->
% io:format("[~s]:General ERROR(~p) Op(~p) ~n ",
%   ["ProcessResults", _Results, _Op ]),
  stats_record:incrGeneralErr( _StatsRecord ).

%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% spawnProc(): Spawn a worker process
%% Recursively count down "Num Processes", spawning a worker until there
%% are "Num Processes" workers launched.
%% "doOp() does the actual work of getting or putting values.
%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
spawnProc(_C, _Ns, _Set, _WriteWeight, _TotalWeight, _NumProcRemain,
  _NumOps, _KeyUpperRange, _KeySize, _ValueSize ) ->
    if 
      _NumProcRemain =< 0 ->
        io:format("Proc spawning done. Please wait for procs to complete.~n");
    true -> 
      {A1, A2, A3} = now(),
      random:seed(A1,A2,A3),
%%  	io:format("~w procs remained to be spawned ~n",[_NumProcRemain]),
      %% Make a Stats Record to carry stats for this work sequence
      SR = stats_record:makeRecord( _NumProcRemain ),
      %% Spawn a worker (then spawn a launcher)
      spawn(asbench, doOp, [_C, _Ns, _Set, _WriteWeight, _TotalWeight,
          _NumProcRemain, _NumOps, _KeyUpperRange, _KeySize, _ValueSize, SR]),
      %% Now spawn another instance of this function to do it all again.
      spawnProc(_C, _Ns, _Set, _WriteWeight, _TotalWeight,
          _NumProcRemain-1, _NumOps, _KeyUpperRange, _KeySize, _ValueSize)
    end.

%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% doOp():  Do the Operation:
%% doOp(Con, NS, Set, WrtWeight, TotalWeight, ProcSeq, NumOp, KeyUpper, SR)
%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Once our Number of ops drops to ZERO, stop the timer, write the report
doOp(_C, _Ns, _Set, _WriteWeight, _TotalWeight, _ProcSeq, 0,
  _KeyUpperRange, _KeySize, _ValueSize, _SR) ->
  %% This printout is not needed long term -- debug only.
  %% io:format("~nDO-OP:E: Proc #~w ending. Timenow=~w ~n", [_ProcSeq, now()]),
  %% io:format("Final Stats:  ~n" ),
  stats_record:showStats( _SR ),
  R = aerospike:stopwatch(report),
  io:format("Stopwatch readout: ~w ~n", [R]),
  [{readhistogram, Rh},{writehistogram, Wh}, {seconds, S}] =
    aerospike:histogram(report),
  io:format("  ~s ~n  ~s ~n  lapse (sec) = ~w ~n", [Rh, Wh, S]),
  ok;

%% -----------------------------------------------------------------------
%% While the Number of Ops > ZERO, Do more ops (get or put), based on the
%% Lottery (random number) results
doOp(_C, _Ns, _Set, _WriteWeight, _TotalWeight, _ProcSeq, _NumOps,
  _KeyUpperRange, _KeySize, _ValueSize, _SR ) ->
  %% io:format("~nDO-OP:W: Proc#(~w) ending. Timenow(~w) NumOps(~w) ~n",
    %% [_ProcSeq, now(),_NumOps]),
    Lottery = random:uniform(_TotalWeight),
    % Key = random:uniform(_KeyUpperRange),
    % Value = random:uniform(Lottery),
    
    Key = generateKey( _KeyUpperRange, _KeySize ),
    Value = generateValue( Lottery, _ValueSize ),
    if 
        Lottery > _WriteWeight ->
%         io:format("Do Op: ProcSeq(~p) GET(~s) ~n", [_ProcSeq,Key]),
          ReadResult = aerospike:getAll(_C, _Ns, _Set, Key, 0),
%         io:format("GET Results for Key(~s)<<~p>> ~n", [Key, ReadResult] ),
          UpdatedStats = processResults( ReadResult, read, _SR ),
          ok;
        true ->
        Record = [{"benchbin1", Value}],
%         io:format("Do Op: ProcSeq(~w) PUT(Key[~s]:Rec[~p]) ~n",
%           [_ProcSeq,Key,Record]),
        WriteResult =
          aerospike:put(_C, _Ns, _Set, Key, Record, 0),
%         io:format("PUT Results for Key(~s)<<~p>>~n", [Key, WriteResult] ),
          UpdatedStats = processResults( WriteResult, write, _SR ),
          ok
    end,
%   io:format("Calling doOp() with Proc(~p) NumOps(~p) UpdatedStats(~p) ~n",
%     [_ProcSeq, _NumOps-1, UpdatedStats ]),
    doOp(_C, _Ns, _Set, _WriteWeight, _TotalWeight, _ProcSeq,
        _NumOps-1, _KeyUpperRange, _KeySize, _ValueSize, UpdatedStats ).

%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Canned Tests that we can just invoke once from the command line.
%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% ------------------------------------------------------------------------
%% Test 0: Small keys/values,  few Ops/Procs
%% ------------------------------------------------------------------------
test0() ->
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
  KeyUpperRange = 2,
  KeySize = 2,
  ValueSize = 128,
  io:format("~n<><><> Canned Test (~w) <><><>~n",[TestNumber]),
  io:format("::Host(~s) Port(~w) NameSpace(~s) Set(~s) WriteWeight(~w) ~n",
    [Host, Port, Namespace, Set, WriteWeight] ),
  io:format("::ReadWeight(~w) NumProcs(~w) NumOps(~w) KeyUpperRange(~w) ~n",
    [ReadWeight, NumProc, NumOps, KeyUpperRange]),
  io:format("::KeySize(~w) ValueSize(~w) ~n", [KeySize, ValueSize]),

  %% Do the deed
  bench(Host, Port, Namespace, Set, WriteWeight, ReadWeight, NumProc,
    NumOps, KeyUpperRange, KeySize, ValueSize ).


%% ------------------------------------------------------------------------
%% Test 1: Built keys/values,  few Ops/Procs
%% ------------------------------------------------------------------------
test1() ->
  %% Set the parms
  TestNumber = 1,
  Host = "127.0.0.1",
  Port = 3000,
  Namespace = "test",
  Set = "test_set",
  WriteWeight = 50,
  ReadWeight = 50,
  NumProc = 10,
  NumOps = 100,
  KeyUpperRange = 20,
  KeySize = 20,  %% Build a Key -- A Number with Padding
  ValueSize = 100,  %% Build a Value -- A Number with Padding
  io:format("~n<><><> Canned Test (~w) <><><>~n",[TestNumber]),
  io:format("::Host(~s) Port(~w) NameSpace(~s) Set(~s) WriteWeight(~w) ~n",
    [Host, Port, Namespace, Set, WriteWeight] ),
  io:format("::ReadWeight(~w) NumProcs(~w) NumOps(~w) KeyUpperRange(~w) ~n",
    [ReadWeight, NumProc, NumOps, KeyUpperRange]),
  io:format("::KeySize(~w) ValueSize(~w) ~n", [KeySize, ValueSize]),

  %% Do the deed
  bench(Host, Port, Namespace, Set, WriteWeight, ReadWeight, NumProc,
    NumOps, KeyUpperRange, KeySize, ValueSize ).

%% ------------------------------------------------------------------------
%% <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF>
%% ------------------------------------------------------------------------
