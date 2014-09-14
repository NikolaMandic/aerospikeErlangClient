%%% Copyright (C) 2013  by Aerospike.  All rights reserved.  
%%
%% -------------------------------------------------------------------------
%% Aerospike Erlang Application Record Example
%% -------------------------------------------------------------------------
%% Manage Records in the Aerospike Citrusleaf Erlang Client.
%% Create Records, flatten them (into binary), store them, retrieve them,
%% Modify them re-write them -- exploiting some of the Aerospike Write
%% Policy settings (e.g. Unique, Generation, Timeout, etc).
%%
%% Record Structure  (type name "addressRecord")
%%
%%          Field Name      |  possible values
%%         --------------------------------------------
%%          name:           |  String (default undefined)
%%          streetAddress:  |  String (default undefined)
%%          city:           |  String (default undefined)
%%          state:          |  String (default undefined)
%%          zipCode:        |  String (default undefined)
%%          cellPhone:      |  List of Integers (default is [])
%%          homePhone:      |  List of Integers (default is [])
%%          busPhone:       |  List of Integers (default is [])
%%          email:          |  String
%%          notes:          |  Dictionary (Key, Value) (default empty list)
%%
%%

-record(addressRecord,
  { name,
    streetAddress,
    city,
    state,
    zipCode,
    cellPhone = [],
    homePhone = [],
    busPhone = [],
    email,
    notes = []}).

%% <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF>
