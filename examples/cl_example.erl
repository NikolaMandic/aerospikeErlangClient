-module(cl_example).
-export([runAll/0, runAll/3]).

%% Import Citrusleaf, which, on load, will load up the NIF module.
-import(citrusleaf,
  [connect/2, put/6, get/6, getAll/5, delete/5, getAll/5, clinfo/1, clinfo/2]).

-on_load(init/0).

%% -----------------------------------------------------------------------
%% 
%% -----------------------------------------------------------------------
init() ->
   io:format("Starting in init() ~n"),
   io:format("Command to run example: cl_example:runAll(\"Host\",
					Port, NameSpace). ~n"),
   io:format("Done with  init() ~n"),
	 ok.

%% Entry point to run all tests. User must provide, Port, NameSpace
runAll(Host, Port, NS) ->
    Set = "example_set",
    Key = "example_key",
    {ok, C} = eg_connect(Host, Port),
    eg_delete(C, NS, Set, Key),
    eg_put(C, NS, Set, Key, [{"bin1", "value1"}, {"bin2", "value2"}]),
    eg_getAll(C, NS, Set, Key),
    eg_get(C, NS, Set, Key, ["bin2", "bin1"]),
    eg_clinfo_single(C, "namespaces"),
    eg_clinfo(C),
    ok.

%% Convenience shortcut for out-of-the-box environment
runAll() ->
		io:format("Calling main test with local, 3000, test ~n"),
    runAll("127.0.0.1", 3000, "test").

%% Individual examples
%%

eg_connect(Host, Port) ->
    io:format("================== connect ============== ~n~n"),
    io:format("connect to host=~s port=~w ~n", [Host, Port]),
    citrusleaf:connect(Host, Port).

eg_delete(C, NS, Set, Key) ->
    io:format("================== delete ============== ~n~n"),
    io:format("Deleting set=~s key=~s ~n", [Set, Key]),
    citrusleaf:delete(C, NS, Set, Key, null).

eg_put(C, NS, Set, Key, Values) ->
    io:format("================== put ============== ~n~n"),
    io:format("put key=~s ~n", [Key]),
    citrusleaf:put(C, NS, Set, Key, Values, null).

eg_getAll(C, NS, Set, Key) -> 
    io:format("================== getAll ============== ~n~n"),
    io:format("getAll key=~s ~n", [Key]),
    citrusleaf:getAll(C, NS, Set, Key, 0).

eg_get(C, NS, Set, Key, BinNameList) -> 
    io:format("================== get ============== ~n~n"),
    io:format("get key=~s ~n", [Key]),
    citrusleaf:get(C, NS, Set, Key, BinNameList, 0).

eg_clinfo_single(C, Prop) ->
    io:format("================== clinfo (one property) ============== ~n~n"),
    io:format("clinfo. prop=~s ~n", [Prop]),
    citrusleaf:clinfo(C, Prop).
 
eg_clinfo(C) ->
    io:format("================== clinfo () ============== ~n~n"),
    io:format("clinfo ~n"),
    io:format("clinfo commented out for now ~n").
    %% citrusleaf:clinfo(C).
