%%% Copyright (C) 2013  by Citrusleaf.  All rights reserved.  
%%% THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
%%%  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.        
%%
%% This file contains the Elang Client Interface for the Aerospike KVS.
%% 1/10/2013 tjl:
%%
%% Here is a summary of the Citrusleaf API:
%% 
%% (*) connect/0:     Connect to the database using default values
%%                    (Host:=127.0.0.1, Port=3000)
%% (*) connect/2:     Connect to the database, specifying Host, Port
%% (*) addHost/4:     Add a host to the cluster
%% (*) shutdown/1:    Shutdown a single connection
%% (*) shutdownAll/0: Shutdown all connections
%% (*) clinfo/1:      Get property/status info from the ASD/CLD Server
%% (*) clinfo/2:      Get property/status info from the ASD/CLD Server
%% (*) init/0:        Initialize the server and module (Done on load)
%% (*) put/6:         Write a Record to the Aerospike KVS
%% (*) get/6:         Get specified bins from a specified KVS Record
%% (*) getAll/5:      Get all bins from a specified KVS Record
%% (*) delete/5:      Delete a specified KVS Record
%% (*) histogram/1:   Start Histogram, or get HS Report
%% (*) stopwatch/1:   Start, Stop the stopwatch, or get the SW Report

% Define this module and file:  citrusleaf.erl
-module(citrusleaf).

%% These are the functions that we define here and will make available
%% to any caller who wants to import us.
-export([connect/0, connect/2, addhost/4, shutdown/1, shutdownAll/0,
	clinfo/1, clinfo/2, init/0, put/6, get/6, getAll/5, delete/5,
	histogram/1, stopwatch/1]).

%% Include our record definitions.
-include("citrusleaf.hrl").

%% When this module is loaded, load the citrusleaf Erlang Library
%% (file citrusleaf_nif.so), which is expected to be in the current directory.
%% Then, perform the Database connect so that we're ready for any Aerospike
%% function calls.
-on_load(init/0).

%% -----------------------------------------------------------------------
%% Initialize the system -- load in the nif, connect to the DB
%% -----------------------------------------------------------------------
init() ->
  io:format(">> init() with citrusleaf.erl ~n" ),
  %% load the nif file, then if ok, init the system
	ok = erlang:load_nif("./citrusleaf_nif", 0).
  % Something strange here -- system won't recognize clgwinit
  %% citrusleaf:clgwinit().

%%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%  >>>>>>>>>>>>  Begin Native Implemented Functions (NIFs) <<<<<<<<<<<<<
%%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% In all of the APIs below, if errors are encountered the response would be
%% in one of these 2 forms:
%%    General error =>
%%            {error, GeneralErrorDesc}
%%    Citrusleaf server responsed with error =>
%%            {citrusleaf_error, {Cl_error_code, CitrusleafErrorDesc}}
%%
%%
%% -----------------------------------------------------------------------
%% Name: connect()
%% Description: Connect to a citrusleaf cluster using default values
%% Usage:
%%    {Result, ConnectHandle} = connect()
%% NOTES:
%% (1) Default values are: Host="127.0.0.1", Port=3000
%% -----------------------------------------------------------------------
connect() ->
  io:format("connect( Host(127.0.0.1) Port(3000) )~n"),
	connect("127.0.0.1", 3000).

%% -----------------------------------------------------------------------
%% Name: connect( Host, Port )
%% Description: Connect to a citrusleaf cluster using specified parameters
%% Usage:
%%    {Result, ConnectHandle} = connect("127.0.0.1", 3000).
%% 
%% -----------------------------------------------------------------------
connect(_Host, _Port) ->
  io:format(">> Connect( Host(~s) Port(~w) ) ~n",[_Host, _Port]),
  citrusleaf:connect( _Host, _Port).

%% -----------------------------------------------------------------------
%% Name: addhost(Connection, Host, Port, Timeout)
%% Description: Add a host to the cluster for reference purpose
%% Usage:
%%   Result = addHost(Connection, "host_name", 3000, 0).
%% Errors:
%%
%% Notes:
%% (1) Timeout_ms = 0 means to use default timeout value.
%% -----------------------------------------------------------------------
addhost(_C, _Host, _Port, _Timeout_ms) ->
  io:format(">> addHost( C(~w) Host(~s) Port(~w) ) ~n",[_C, _Host, _Port]),
  citrusleaf:addHost( _C, _Host, _Port, _Timeout_ms ).

%% -----------------------------------------------------------------------
%% Shut down the citrusleaf client associate with the connection handle
%% Usage:
%%    Result = shutdown(C)
%% Errors:
%% -----------------------------------------------------------------------
shutdown(_C) ->
  io:format(">> Shutdown( Conn_Handle(~w)):: ~n",[_C]),
  citrusleaf:shutdown( _C ).

%% -----------------------------------------------------------------------
%% Shut down all the connected citrusleaf clients. Release all citrusleaf
%% client data structures.
%% Usage:
%%    Result = shutdownAll(C).
%% Errors:
%% -----------------------------------------------------------------------
shutdownAll() ->
  io:format(">> ShutdownAll() ~n"),
  citrusleaf:shutdownAll().

%% -----------------------------------------------------------------------
%% Name: clinfo( Connection, PropertyNameList )
%% Description:
%% Get the cluster info of the specified properties. Properties names are
%% comma seperated
%% Usage:
%%    InfoString = clinfo(C, "service,status").
%% -----------------------------------------------------------------------
clinfo(_C, _CommaSeparatedPropertyNames) ->
  io:format(">> clinfo( Connection(~w) Props(~w) ):: ~n",
   [ _C, _CommaSeparatedPropertyNames ]),
	citrusleaf:clinfo(_C, _CommaSeparatedPropertyNames ).

%% Perform clinfo with a null list -- which means "all"
clinfo(_C) ->
  io:format(">> clinfo( C(~w) ):: ~n", [_C] ),
	clinfo(_C, "").

%% -----------------------------------------------------------------------
%% Name: put(Connection, Namespace, Set, Key, Values, clwp )
%% Description: Put values into the database
%% Usage:
%%  Result = put(C, "testns", "myset", "mykey", [{"binnm1", "value1"}, ... ], 0)
%% Errors and Return Values:
%% (*) 
%%
%% Notes:
%% (1) Use the clwp record as Clwp. It is the  equivalent of
%%     C cl_write_parameters.  Setting Clwp= 0 means to use the default
%% -----------------------------------------------------------------------
put(_C, _NameSpace, _Set, _Key, _Values, _Clwp) ->
  io:format(">> put( C(~w) NS(~s) S(~s) K(~s) VAL(~w) CLWP(~w):: ~n",
  [_C, _NameSpace, _Set, _Key, _Values, _Clwp ]),
  citrusleaf:put(_C, _NameSpace, _Set, _Key, _Values, _Clwp).

%% -----------------------------------------------------------------------
%% Name: get(Connection, Namespace, Set, Key, BinNames, Timeout )
%% Description: Get specified values from the database, for a given key
%% Usage:
%%   [{BinName1, Value1}, {BinName2, Value} ...] =
%%     get(C, "test", "myset", "mykey", ["binname1", "binname2", ...], TimeOut).
%% Errors:
%% Notes:
%% -----------------------------------------------------------------------
get(_C, _NameSpace, _Set, _Key, _BinNames, _Timeout_ms) ->
io:format(">> get( C(~w) NS(~s) S(~s) K(~s) Bins(~w) Timeout(~w):: ~n",
  [_C, _NameSpace, _Set, _Key, _BinNames, _Timeout_ms ]),
  citrusleaf:get(_C, _NameSpace, _Set, _Key, _BinNames, _Timeout_ms ).

%% -----------------------------------------------------------------------
%% Name: getAll(Connection, Namespace, Set, Key, Timeout )
%% Description: Get All values from the database, for a given key
%% Usage:
%%   [{BinName1, Value1}, {BinName2, Value2} ...] =
%%     get(C, "testns", "myset", "mykey", Timeout).
%% Errors:
%% Notes:
%% -----------------------------------------------------------------------
getAll(_C, _NameSpace, _Set, _Key, _Timeout_ms) ->
io:format(">> getAll( C(~w) NS(~s) Set(~s) Key(~s) Timeout(~w):: ~n",
  [_C, _NameSpace, _Set, _Key, _Timeout_ms ]),
  citrusleaf:getAll(_C, _NameSpace, _Set, _Key,  _Timeout_ms ).

%% -----------------------------------------------------------------------
%% Name: delete( Connection, Namespace, Set, Key, Timeout )
%% Description: Delete a key (and record) from the database
%% Usage:
%%    Result = delete(C, "testns", "myset", "mykey", CpWOpts).
%%
%% Use the clwp record as Clwp. It is the  equivalent of C cl_write_parameters
%% setting Clwp= 0 means to use the default
%% -----------------------------------------------------------------------
delete(_C, _NameSpace, _Set, _Key, _CpWOpts) ->
io:format(">> delete( C(~w) NS(~s) Set(~s) Key(~s) CpWOpts(~w):: ~n",
  [_C, _NameSpace, _Set, _Key, _CpWOpts  ]),
  delete(_C, _NameSpace, _Set, _Key, _CpWOpts ).

%%  -----------------------------------------------------------------------
%% Name: histogram
%% Description: Count the reads and writes
%% Parameters:
%% (*) Command:
%%      start: To start the histogram, and the counting of reads and writes
%%      report: Produce a report of the data since the histogram starts. This
%%              command does not stop the counter
%% -----------------------------------------------------------------------
histogram( Command ) when Command == start ->
  io:format(">>> Histogram( start ):: ~n"),
  citrusleaf:historgram(start);

histogram( Command ) when Command == report ->
  io:format(">>> Histogram( report ):: ~n"),
  citrusleaf:historgram(report).

%%   -----------------------------------------------------------------------
%% Name: stopwatch
%% Description: stopwatch counts all reads and writes to citrusleaf DB
%% Parameters:
%% (*) Commands
%%      start: To start the timer, and the counting
%%      stop:  To stop the timer, and product a report 
%%      report: Produce a report of the data since the counting starts. This
%%              command does not stop the timer.
%% -----------------------------------------------------------------------
stopwatch( Command ) when Command == start ->
  io:format(">>> Stopwatch( start ):: ~n"),
  citrusleaf:stopwatch(start);

stopwatch( Command ) when Command == stop ->
  io:format(">>> Stopwatch( stop ):: ~n"),
  citrusleaf:stopwatch(stop);

stopwatch( Command ) when Command == report ->
  io:format(">>> Stopwatch( report ):: ~n"),
  citrusleaf:stopwatch(report).

%%%
%%  >>>>>>>>>>>>>>  end Native Implemented Functions (NIFs) <<<<<<<<<<<<<<
%%%

%% -----------------------------------------------------------------------
%% Convenience functions
%% -----------------------------------------------------------------------

%% -----------------------------------------------------------------------
%% NIF not implemented error reporting
%% -----------------------------------------------------------------------
% Not currently needed
% not_implemented_error(Line) ->
% 	erlang:error({"NIF not (yet) implemented in citrusleaf_nif at line", Line}).


%% <EOAEAF> <EOAEAF> <EOAEAF> <EOAEAF> <EOAEAF> <EOAEAF> <EOAEAF> <EOAEAF> 
%%
%% >>>>>>>>>>>>>>>  END OF Aerospike ERLANG API File <<<<<<<<<<<<<<<<<<<<<
%%
%% <EOAEAF> <EOAEAF> <EOAEAF> <EOAEAF> <EOAEAF> <EOAEAF> <EOAEAF> <EOAEAF> 
