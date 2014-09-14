%%% Copyright (C) 2013  by Aerospike.  All rights reserved.  
%% -------------------------------------------------------------------------
%% Erlang Application Hello World Example
%% -------------------------------------------------------------------------
%% Simple steps in the Hello World Application
%% (1) Connect to Aerospike
%% (2) Write (put) a "hello world" record in Aerospike
%% (3) Read (get) a "hello world" record from Aerospike
%% (4) Remove (delete) the "hello world" record from Aerospike
%% (5) Disconnect (shutdown) 
%%

%% Define this module:  helloWorld.erl
-module(helloWorld).

%% Import the Aerospike API module and Define those functions from it that 
%% we will use in this module
-import(aerospike,
   [connect/2, put/6, getAll/5, delete/4, shutdownAll/0 ]).

%% Expose the following to be called either by other modules or by the
%% client shell (command line).
-export([ hello/0 ]).
    
%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% hello: Run our sample program
%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% ------------------------------------------------------------------------
hello() ->
  io:format("Hello From the World of Erlang ~n"),
  %% Set the parms for attaching to a local Aerospike Server
  Host = "127.0.0.1",  %% Assume a server is installed and running locally
                       %% ==>  /etc/init.d/citrusleaf start
  Port = 3000,
  NS = "test",
  Set = "test_set",
  {ok, C } = aerospike:connect( Host, Port ),
  Key = "hello",
  MyRecord = [{"BinOne", "Hello World"}],
  WriteOps = 0,

  % Write to the Aerospike Server
  aerospike:put( C, NS, Set, Key, MyRecord, WriteOps),

  % Read from the Aerospike Server
  GetResults = aerospike:getAll( C, NS, Set, Key, 0),
  io:format("Get Results(~p) ~n", [GetResults]),

  % Delete from the Aerospike Server
  aerospike:delete( C, NS, Set, Key, 0 ),

  aerospike:shutdownAll(),
  io:format("Goodbye From the World of Erlang ~n"),
  ok. %% Leave on a high note.

%% ------------------------------------------------------------------------
%% <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF>
%% ------------------------------------------------------------------------
