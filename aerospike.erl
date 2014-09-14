%% ==========================================================================
%% Copyright (C) 2013  by Aerospike Inc.   All rights reserved.  
%% THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
%% ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.        
%% ==========================================================================
%%
%% This file contains the Erlang Client Interface for the Aerospike Database.
%%
%% Here is a summary of the Aerospike API:
%% 
%% (*) connect/0:     Connect to the database using default values
%%                    (Host:=127.0.0.1, Port=3000)
%% (*) connect/2:     Connect to the database, specifying Host, Port
%% (*) addhost/4:     Add a host to the cluster
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
%% ===================================================================
%% NOTES:
%% (1) The Erlang Native Implemented Functions (NIF) work as follows:
%% - The Aerospike Erlang client comprises a set of functions,
%%   such as connect/2, put/6, get/6, etc.  Those functions appear
%%   in two places:  the aerospike.erl file and the aerospike_nif.c
%%   file.
%% - The Aerospike Erlang functions (in aerospike.erl) basically serve
%%   as interfaces for Erlang applications, but the real function is
%%   implemented by the aerospike_nif.c functions.
%% - When the aerospike_nif.so library is loaded, the "empty" functions
%%   in the aerospike.erl file are replaced by the nif functions.
%% - In the event that the load fails (for whatever reason), the
%%   "empty" Erlang functions are left in the aerospike.erl file, and
%%   then throw a simple "function not loaded" error atom.
%% - Since the aerospike_nif.so library is loaded automatically on
%%   use or import of the aerospike.erl file, everything should work
%%   as expected unless there's a problem with the library file.
%% ===================================================================

% Define this module and file:  aerospike.erl
-module(aerospike).

%% These are the functions that we define here and will make available
%% to any caller who wants to import us.
-export([connect/0, connect/2, addhost/4, shutdown/1, shutdownAll/0,
	clinfo/1, clinfo/2, init/0, put/6, get/6, getAll/5, delete/5,
	histogram/1, stopwatch/1]).

%% Include our record definitions.
-include("aerospike.hrl").

%% When this module is loaded, load the aerospike Erlang Library
%% (file aerospike_nif.so), which is expected to be in the current directory.
%% Then, perform the Database connect so that we're ready for any Aerospike
%% function calls.
-on_load(init/0).

%% -----------------------------------------------------------------------
%% Initialize the system -- load in the nif, connect to the DB
%% -----------------------------------------------------------------------
init() ->
  io:format(":: Init() :: Loading aerospike NIF Module ~n" ),
  %% load the nif file, then if ok, init the system
	ok = erlang:load_nif("./aerospike_nif", 0).

%%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%  >>>>>>>>>>>>  Begin Native Implemented Functions (NIFs) <<<<<<<<<<<<<
%%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% In all of the APIs below, if errors are encountered the response would be
%% in one of these 2 forms:
%%    General error =>
%%            {error, GeneralErrorDesc}
%%    Aerospike server responsed with error =>
%%            {citrusleaf_error, {Cl_error_code, AerospikeErrorDesc}}
%%
%% -----------------------------------------------------------------------
%% Name: connect()
%% Description: Connect to a aerospike cluster using default values
%% Usage:
%%    {Result, ConnectHandle} = connect()
%% NOTES:
%% (1) Default values are: Host="127.0.0.1", Port=3000
%% -----------------------------------------------------------------------
connect() ->
  io:format("connect( Default Host(127.0.0.1) Default Port(3000) )~n"),
  % Call the "real" connect() function.
	connect("127.0.0.1", 3000).

%% -----------------------------------------------------------------------
%% Name: connect( Host, Port )
%% Description: Connect to a aerospike cluster using specified parameters
%% Parameters:
%%   Host: The IP Name or IP Address of a server running Aerospike 
%%   Port: The Port for Aerospike (usually 3000)
%% Usage:
%%    {Result, ConnectHandle} = connect("127.0.0.1", 3000).
%% 
%% -----------------------------------------------------------------------
connect(_Host, _Port) ->
% io:format(">> Connect( Host(~s) Port(~w) ) ~n",[_Host, _Port]),
% aerospike:connect( _Host, _Port).
  'function_connect_not_loaded'.  % This gets replaced by the NIF call

%% -----------------------------------------------------------------------
%% Name: addhost(Connection, Host, Port, TimeoutMS)
%% Description: Add a host to the cluster for reference purpose
%% Parameters:
%%   Connection: the connection ID from the initial connect(Host, Port) call
%%   Host: IP Name or IP Address of another Server in the cluster
%%   Port: Port Number of another Server in the cluster
%%   TimeoutMS: Number of Milliseconds to wait for the connection. 
%%   
%% Usage:
%%   Result = addhost(Connection, "host_name", 3000, 0).
%% Errors:
%%
%% Notes:
%% (1) TimeoutMS = 0 means: use default timeout value.
%% -----------------------------------------------------------------------
addhost(_C, _Host, _Port, _Timeout_ms) ->
% io:format(">> addhost( C(~w) Host(~s) Port(~w) ) ~n",[_C, _Host, _Port]),
% aerospike:addhost( _C, _Host, _Port, _Timeout_ms ).
  'function_addhost_not_loaded'. % This gets replaced by the NIF call on load

%% -----------------------------------------------------------------------
%% Name: shutdown( Connection )
%% Description: Shut down the aerospike client associate with the connection handle
%% Parameters:
%%   Connection: the connection ID from the initial connect(Host, Port) call
%% Usage:
%%    Result = shutdown(C)
%% Errors:
%% -----------------------------------------------------------------------
shutdown(_C) ->
% io:format(">> Shutdown( Conn_Handle(~w)):: ~n",[_C]),
% aerospike:shutdown( _C ).
  'function_shutdown_not_loaded'. % This gets replaced by the NIF call on load

%% -----------------------------------------------------------------------
%% Shut down all the connected aerospike clients. Release all aerospike
%% client data structures.
%% Parameters:
%%  None
%% Usage:
%%    Result = shutdownAll(C).
%% Errors:
%% -----------------------------------------------------------------------
shutdownAll() ->
% io:format(">> ShutdownAll() ~n"),
% aerospike:shutdownAll().
  'function_shutdownAll_not_loaded'. % This gets replaced by the NIF call on load

%% -----------------------------------------------------------------------
%% Name: clinfo( Connection, PropertyNameList )
%% Description:
%% Get the cluster info of the specified properties. Properties names are
%% comma seperated
%% Parameters:
%%   Connection: the connection ID from the initial connect(Host, Port) call
%%   PropertyList: The list of properties (comma separated) to return
%% Usage:
%%    InfoString = clinfo(C, "service,status").
%% -----------------------------------------------------------------------
clinfo(_C, _CommaSeparatedPropertyNames) ->
% io:format(">> clinfo( Connection(~w) Props(~w) ):: ~n",
%  [ _C, _CommaSeparatedPropertyNames ]),
%  aerospike:clinfo(_C, _CommaSeparatedPropertyNames ).
  'function_clinfo_not_loaded'.

%% Perform clinfo with a null list -- which means "all"
clinfo(_C) ->
  io:format(">> clinfo( C(~w) ):: ~n", [_C] ),
	clinfo(_C, "").

%% -----------------------------------------------------------------------
%% Name: put(Connection, Namespace, Set, Key, Values, Clwp )
%% Description: Put values into the database
%% Parameters:
%%   Connection: the connection ID from the initial connect(Host, Port) call
%%   Namespace: The Namespace into which this put value will be written
%%       Note that Namespace is defined in the aerospike server config file
%%       and that the Namespace "test" is a defined Namespace in the 
%%       default /etc/citrusleaf.conf server config file.
%%   Set: The Set name for this grouping of records
%%   Key: The unique key (e.g. "abc") for this record
%%   Values: List of pairs:[{BinName1, Value1},{BinName2,Value2}]
%%   Clwp: Write Parameters (see documentation)
%%   
%% Usage:
%%  Result = put(C, "testns", "myset", "mykey", [{"binnm1", "value1"}, ... ], clwp)
%% Errors and Return Values:
%% (*) 
%%
%% Notes:
%% (1) Use the clwp record as Clwp. It is the  equivalent of
%%     C cl_write_parameters.  Setting Clwp = 0 means to use the default
%%     values.  (See Documentation)
%% -----------------------------------------------------------------------
put(_C, _NameSpace, _Set, _Key, _Values, _Clwp) ->
% io:format(">> put( C(~w) NS(~s) S(~s) K(~s) VAL(~w) CLWP(~w):: ~n",
% [_C, _NameSpace, _Set, _Key, _Values, _Clwp ]),
% aerospike:put(_C, _NameSpace, _Set, _Key, _Values, _Clwp).
  'function_put_not_loaded'. % This gets replaced by the NIF call on load

%% -----------------------------------------------------------------------
%% Name: get(Connection, Namespace, Set, Key, BinNames, TimeoutMS )
%% Description: Get specified values from the database, for a given key
%% Parameters:
%%   Connection: the connection ID from the initial connect(Host, Port) call
%%   Namespace: The Namespace into which this put value will be written
%%       Note that Namespace is defined in the aerospike server config file
%%       and that the Namespace "test" is a defined Namespace in the 
%%       default /etc/citrusleaf.conf server config file.
%%   Set: The Set name for this grouping of records
%%   Key: The unique key (e.g. "abc") for this record
%%   BinNames: List of bin names:e.g. [BinName1, BinName2]
%%   TimeoutMS: Timeout (in milliseconds) to wait (0 = forever)
%% Usage:
%%   [{BinName1, Value1}, {BinName2, Value} ...] =
%%     get(C, "test", "myset", "mykey", ["binname1", "binname2", ...], TimeOutMS).
%% Errors:
%% Notes:
%% -----------------------------------------------------------------------
get(_C, _NameSpace, _Set, _Key, _BinNames, _Timeout_ms) ->
% io:format(">> get( C(~w) NS(~s) S(~s) K(~s) Bins(~w) Timeout(~w):: ~n",
% [_C, _NameSpace, _Set, _Key, _BinNames, _Timeout_ms ]),
% aerospike:get(_C, _NameSpace, _Set, _Key, _BinNames, _Timeout_ms ).
  'function_get_not_loaded'.  % This gets replaced by the NIF call on load

%% -----------------------------------------------------------------------
%% Name: getAll(Connection, Namespace, Set, Key, TimeoutMS )
%% Description: Get All values from the database, for a given key
%% Parameters:
%%   Connection: the connection ID from the initial connect(Host, Port) call
%%   Namespace: The Namespace into which this put value will be written
%%       Note that Namespace is defined in the aerospike server config file
%%       and that the Namespace "test" is a defined Namespace in the 
%%       default /etc/citrusleaf.conf server config file.
%%   Set: The Set name for this grouping of records
%%   Key: The unique key (e.g. "abc") for this record
%%   TimeoutMS: Timeout (in milliseconds) to wait (0 = forever)
%% Usage:
%%   [{BinName1, Value1}, {BinName2, Value2} ...] =
%%     get(C, "testns", "myset", "mykey", TimeoutMS).
%% Errors:
%% Notes:
%% -----------------------------------------------------------------------
getAll(_C, _NameSpace, _Set, _Key, _Timeout_ms) ->
% io:format(">> getAll( C(~w) NS(~s) Set(~s) Key(~s) Timeout(~w):: ~n",
% [_C, _NameSpace, _Set, _Key, _Timeout_ms ]),
% aerospike:getAll(_C, _NameSpace, _Set, _Key,  _Timeout_ms ).
  'function_getAll_not_loaded'.  % This gets replaced by the NIF call on load

%% -----------------------------------------------------------------------
%% Name: delete( Connection, Namespace, Set, Key, Clwp )
%% Description: Delete a key (and record) from the database
%% Parameters:
%%   Connection: the connection ID from the initial connect(Host, Port) call
%%   Namespace: The Namespace into which this put value will be written
%%       Note that Namespace is defined in the aerospike server config file
%%       and that the Namespace "test" is a defined Namespace in the 
%%       default /etc/citrusleaf.conf server config file.
%%   Set: The Set name for this grouping of records
%%   Key: The unique key (e.g. "abc") for this record
%%   Clwp: Write Parameters (see documentation)
%% Usage:
%%    Result = delete(C, "testns", "myset", "mykey", Clwp).
%%
%% Notes:
%% (1) Use the clwp record as Clwp. It is the  equivalent of the
%%     C cl_write_parameters. Setting Clwp = 0 means to use the defaults.
%% -----------------------------------------------------------------------
delete(_C, _NameSpace, _Set, _Key, _CpWOpts) ->
% io:format(">> delete( C(~w) NS(~s) Set(~s) Key(~s) CpWOpts(~w):: ~n",
% [_C, _NameSpace, _Set, _Key, _CpWOpts  ]),
% delete(_C, _NameSpace, _Set, _Key, _CpWOpts ).
  'function_delete_not_loaded'.  % This gets replaced by the NIF call on load

%%  -----------------------------------------------------------------------
%% Name: histogram
%% Description: Count the reads and writes
%% Parameters:
%% (*) Command:  The possible values are:
%%      start: To start the histogram, and the counting of reads and writes
%%      report: Produce a report of the data since the histogram starts. This
%%              command does not stop the counter
%% -----------------------------------------------------------------------
histogram( Command ) when Command == start ->
  io:format(">>> Histogram( start ):: ~n"),
  aerospike:historgram(start);

histogram( Command ) when Command == report ->
  io:format(">>> Histogram( report ):: ~n"),
  aerospike:historgram(report).

%%   -----------------------------------------------------------------------
%% Name: stopwatch
%% Description: stopwatch counts all reads and writes to aerospike DB
%% Parameters:
%% (*) Command:  The possible values are:
%%      start: To start the timer, and the counting
%%      stop:  To stop the timer, and product a report 
%%      report: Produce a report of the data since the counting starts. This
%%              command does not stop the timer.
%% -----------------------------------------------------------------------
stopwatch( Command ) when Command == start ->
  io:format(">>> Stopwatch( start ):: ~n"),
  aerospike:stopwatch(start);

stopwatch( Command ) when Command == stop ->
  io:format(">>> Stopwatch( stop ):: ~n"),
  aerospike:stopwatch(stop);

stopwatch( Command ) when Command == report ->
  io:format(">>> Stopwatch( report ):: ~n"),
  aerospike:stopwatch(report).

%%%
%%  >>>>>>>>>>>>>>  end Native Implemented Functions (NIFs) <<<<<<<<<<<<<<
%%%


%% <EOAEAF> <EOAEAF> <EOAEAF> <EOAEAF> <EOAEAF> <EOAEAF> <EOAEAF> <EOAEAF> 
%%
%% >>>>>>>>>>>>>>>  END OF Aerospike ERLANG API File <<<<<<<<<<<<<<<<<<<<<
%%
%% <EOAEAF> <EOAEAF> <EOAEAF> <EOAEAF> <EOAEAF> <EOAEAF> <EOAEAF> <EOAEAF> 
