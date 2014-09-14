-module(clbench).

-export([bench/9, doOp/8]).

-import(citrusleaf, [connect/2, put/6, get/6, historgramSwitch/1, historgramDump/2, stopwatch/1]).

bench(Host, Port, Ns, Set, WriteWeight, ReadWeight, NProc, NOp, KeyUpperRange) ->
    TotalWeight = WriteWeight + ReadWeight,
    {ok, C} = citrusleaf:connect(Host, Port),
    citrusleaf:histogram(start),
    citrusleaf:stopwatch(start),
    io:format("Starting benchmark: Timenow=~w (WriteWeight=~w, ReadWeight=~w, Procs#=~w, Ops per proc=~w)~n",
    		[now(), WriteWeight, ReadWeight, NProc, NOp]),
    spawnProc(C, Ns, Set, WriteWeight, TotalWeight, NProc, NOp, KeyUpperRange).


spawnProc(C, Ns, Set, WriteWeight, TotalWeight, NProcRemain, NOp, KeyUpperRange) ->
    if 
        NProcRemain =< 0 ->
            io:format("All proc spawning done. Please wait for procs to complete.~n");
    true -> 
        {A1, A2, A3} = now(),
        random:seed(A1,A2,A3),
  	io:format("~w procs remained to be spawned ~n",[NProcRemain]),
        spawn(clbench, doOp, [C, Ns, Set, WriteWeight, TotalWeight, NProcRemain, NOp, KeyUpperRange]),
        spawnProc(C, Ns, Set, WriteWeight, TotalWeight, NProcRemain-1, NOp, KeyUpperRange)
    end.


doOp(_C, _Ns, _Set, _WriteWeight, _TotalWeight, ProcSeq, 0, _KeyUpperRange) ->
    io:format("~nProc #~w ending. Timenow=~w ~n", [ProcSeq, now()]),
    R = citrusleaf:stopwatch(report),
    io:format("Stopwatch readout: ~w ~n", [R]),
    [{readhistogram, Rh},{writehistogram, Wh}, {seconds, S}] = citrusleaf:histogram(report),
    io:format("  ~s ~n  ~s ~n  lapse (sec) = ~w ~n", [Rh, Wh, S]),
    ok;
doOp(C, Ns, Set, WriteWeight, TotalWeight, ProcSeq, NOp, KeyUpperRange) ->
    Lottery = random:uniform(TotalWeight),
    Key = random:uniform(KeyUpperRange),
    Value = random:uniform(Lottery),
    if 
        Lottery > WriteWeight ->
	    citrusleaf:getAll(C, Ns, Set, Key, 0);
        true ->
            citrusleaf:put(C, Ns, Set, Key, [{"benchbin", Value}], 0)
    end,
    doOp(C, Ns, Set, WriteWeight, TotalWeight, ProcSeq, NOp-1, KeyUpperRange).



