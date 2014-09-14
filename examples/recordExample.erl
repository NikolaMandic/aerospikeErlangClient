%%% Copyright (C) 2013  by Aerospike.  All rights reserved.  
%% -------------------------------------------------------------------------
%% Erlang Application Record Example
%% -------------------------------------------------------------------------
%% Manage Records in the Aerospike Erlang Client.
%% Create Records, serialize them (into binary), store them, retrieve them,
%% Modify them re-write them -- exploiting some of the Aerospike Write
%% Parameter settings (e.g. Unique, Generation, Timeout, etc).
%%
%% Record Structure  (type name "addressRecord")
%%
%%          Field Name           |  possible values
%%         -----------------------------------------------
%%          name:                |  Alpha Numeric
%%          streetAddress:       |  Alpha Numeric
%%          city:                |  Alpha Numeric
%%          state:               |  Alpha
%%          zipCode:             |  Numeric
%%          cellPhone:           |  Alpha Numeric
%%          homePhone:           |  Alpha Numeric
%%          busPhone:            |  Alpha Numeric
%%          email:               |  Character
%%					notes:               |  Character
%%

%% Define this module:  recordExample.erl
-module(recordExample).

%% Include the Aerospike structures and the Address Book Record
-include("aerospike.hrl").
-include("addressRecord.hrl").

%% Import the Aerospike API module and Define those functions from it that 
%% we will use in this module
-import(aerospike,
   [connect/2, put/6, get/6, getAll/5, shutdownAll/0 ]).

%% Expose the following to be called either by other modules or by the
%% client shell (command line).
-export([
    newAddressRecord/0,   %% Create a new addressRecord record
    newAddressRecord/2,   %% Create a new addressRecord record
    newAddressRecord/3,   %% Create a new addressRecord record
    newAddressRecord/10,  %% Create a new addressRecord record
    fetchRecords/5,       %% Fetch a set of records from a keylist
    printAddress/1,       %% Print addressRecord record
    updateRecords/5,      %% Modify a field in a list of records
    printKeyList/1,       %% Print a list of keys recursively
    storeRecord/5,        %% Store a single record, return the key
    storeRecord/6,        %% Store a single record, Use caller's key
    deleteRecords/5,      %% Delete records matching the KeyList
    populateRecordList/0, %% Create some sample records
    serializeAndStoreRecords/4, %% Take a list of records, serialize them
                          %% into binary, store them in Aerospike, and
                          %% return the list of keys to find them again.
    runWriteParameterExperiments/4, %% Test the various write parms
    newWriteParameterRecord/1, %% Set the Write Parameters (T/F)
    newWriteParameterRecord/2, %% Set the Write Parameter Parameters (Parm=Value)
    newWriteParameterRecord/9, %% The Full Monte -- set ALL of the parms
    test0/0               %% Run some simple tests on these functions
  ]).

%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% newAddress: Create a new address record.
%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
newAddressRecord() -> 
  #addressRecord{name = "test Record", cellPhone = "9998881234"}.

newAddressRecord( _Name, _CellPhone ) ->
  #addressRecord{name = _Name, cellPhone = _CellPhone}.

newAddressRecord( _Name, _CellPhone, _BusinessPhone ) ->
  #addressRecord{name = _Name, cellPhone = _CellPhone}.

newAddressRecord( _Name, _StreetAddr, _City, _State, _ZipCode,
                  _CellPhone, _HomePhone, _BusinessPhone,
                  _Email, _Notes ) ->

  #addressRecord{name = _Name,
    streetAddress = _StreetAddr,
    city = _City,
    state = _State,
    zipCode = _ZipCode,
    cellPhone = _CellPhone,
    homePhone = _HomePhone,
    busPhone =  _BusinessPhone,
    email = _Email,
    notes = _Notes }. %% Note: We just overwrite, we don't merge or append

%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% printAddress: Create a string representation of the record, 
%% print it with io:format, and return it as the function value.
%% NOTE: We might want to add a conditional that says "Print" or "DO Not Print"
%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
printAddress( #addressRecord{
    name=_Name,
    streetAddress=_StreetAddress,
    city=_City,
    state=_State,
    zipCode=_ZipCode,
    cellPhone=_CellPhone,
    homePhone=_HomePhone,
    busPhone=_BusPhone,
    email=_Email,
    notes=_Notes}) ->
    ReturnString =  io_lib:fwrite("Address Record Contents: " ++
      "Name(~p) Address(~p) City(~p) State(~p) Zip(~p) " ++
      "Cell(~p) HomePhone(~p) Bus Phone(~p) email(~p) Notes(~p)~n",
      [_Name, _StreetAddress, _City, _State, _ZipCode, _CellPhone, _HomePhone,
        _BusPhone, _Email, _Notes]),
%    io:format("~p~n",[ReturnString]),
      ReturnString. %% Return the string we just created

%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Utility Functions
%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% ------------------
%% PopulateRecordList: Generate some sample records that we can use
%% Usage: {ok, RecordList} = populateRecordList(),
%% ------------------
populateRecordList() ->
% io:format("Building Records ~n"),
  R1 = newAddressRecord( "Bart Simpson", "742 Evergreen Terrace",
  "Springfield", "Unknown", "54321",
  "6665551234", "6665551235", "6665551236", "SmellYou@Later.com",
  [{"Hobby", "Skateboarding"},{"Likes","mischief"}]),

  R2 = newAddressRecord( "Herman Munster", "1313 Mockingbird Lane",
  "Chicago", "Il", "49666", "6665551111", "6665551112", "6665551111",
  "HMunster@Monster.com", [{"ShoeSize"},{"21"}] ),

  R3 = newAddressRecord( "Lisa Simpson", "742 Evergreen Terrace",
  "Springfield", "Unknown", "54321",
  "6665552222", "6665552222", "6665552222", "Lisa@Sax.org",
  [{"Hobby", "Music"},{"Likes","family"}] ),

  R4 = newAddressRecord( "Peter Griffin", "309 S Pleasant",
    "Peoria", "Illinois", "61603",
               "3094948803", "3096726855", "6165553333",
               "pgriffin@fightclub.com", [{"Hobby","Imagination"}] ),

  R5 = newAddressRecord( "Ned Flanders", "738 Evergreen Terrace",
    "Springfield", "Unknown", "54321",
                6215554444, 6215554443, 6215554442,
                "nedarama@oralrobers.edu", 
                [{"Favorite Activity", "Prayer"},{"Interests", "Leftorium"}] ),

  R6 = newAddressRecord( "Hershal Krustofsky",
    "321 Main Street", "Springfield", "Unknown", "54321",
              "6785556661", "6785556662", "6785556663",
              "Krusty@laughter.com", [{"Hates","kids"}] ),

              {ok, [R1, R2, R3, R4, R5, R6]}.

% ------------------------------------------------------------------------
% Build a useful (unique) key from the record fields
% ------------------------------------------------------------------------
extractKey( _Record ) when record( _Record, addressRecord ) ->
  lists:flatten(io_lib:fwrite("~s",[_Record#addressRecord.name])); 

% Just throw the atom "bad_record_type" if we're passed the wrong type
extractKey( _Record ) when true ->
  bad_record_type.

%% ------------------------------------------------------------------------
%% StoreRecord(Connect, NameSpace, Set, Record, WriteOps)
%% ------------------------------------------------------------------------
storeRecord( _C, _NS, _Set, _Record, _WriteOps) ->
  Key = extractKey( _Record ),
  FlattenedRecord = term_to_binary(_Record),
% io:format("Calling: PUT( C(~p), NS(~p) Set(~p) Key(~p) Record(~p) ~n",
%   [_C, _NS, _Set, Key, _Record ]),
% io:format("PUT( C(~p), NS(~p) Set(~p) Key(~p) Record(~p) Flattened(~p) ~n",
%   [_C, _NS, _Set, Key,_Record, FlattenedRecord ]),
  
  AS_Record = [{"AddrRec", FlattenedRecord}],
  PutResults = aerospike:put(_C, _NS, _Set, Key, AS_Record, _WriteOps),
% io:format("PUT RESULTS: ~p ~n", [PutResults]),

% % Debug lines to verify the PUT -- Comment out for general use
% io:format("Calling GET( C(~p), NS(~p) Set(~p) Key(~p) ~n",
%   [_C, _NS, _Set, Key ]),
% GetResults = aerospike:getAll(_C, _NS, _Set, Key, 0),
% io:format("GET RESULTS::: <<~p>> ~n", [GetResults] ),
% [Head | Tail ] = GetResults,
% io:format("Head(~p) Tail(~p)  ~n", [Head, Tail] ),

% io:format("Calling Convert on flattened Record(~p) Binary(~w)~n",
%   [binary_to_term(FlattenedRecord), FlattenedRecord ]),

% {Label,Binary} = Head,
% io:format("GET RESULTS: Label(~p) ~n Binary(~p) ~n", [Label, Binary]),

% Original = binary_to_term( Binary ),

% io:format("GET RESULTS: Label(~p)  binary to term(~p) ~n", [Label, Original]),

{PutResults, Key}. %% Return the key back to the caller

%% ------------------------------------------------------------------------
%% StoreRecord( Connect, Namespace, Set, Key, Record, WriteOps )
%% Same as above, except that we take the caller's key -- we don't
%% compute it from the record.
%% ------------------------------------------------------------------------
storeRecord( _C, _NS, _Set, _Key, _Record, _WriteOps) ->
  FlattenedRecord = term_to_binary(_Record),
  AS_Record = [{"AddrRec", FlattenedRecord}],
  PutResults = aerospike:put(_C, _NS, _Set, _Key, AS_Record, _WriteOps),
  {PutResults, _Key}. %% Return the key back to the caller

%% ------------------------------------------------------------------------
% serializeAndStoreRecords(Connect, NameSpace, Set, RecordList )
% Usage: {ok, KeyList} = serializeAndStoreRecords( RecordList ), 
%% ------------------------------------------------------------------------
serializeAndStoreRecords( _C, _NS, _Set, _RecordList ) ->
  KeyList = storeList( _C, _NS, _Set, _RecordList, [] ),
  {ok, KeyList}.

%% When there are no more records in the record list, then return
%% the accumulated KeyList.
storeList(_C, _NS, _Set, [], _KeyList ) ->
  io:format("Returning final KeyList(~p) ~n", [_KeyList] ),
  _KeyList;

%% If there are more records in the list, store the head and process the
%% tail recursively
storeList( _C, _NS, _Set, _RecordList, _KeyList ) ->
  [Head | Tail ] = _RecordList,
% io:format("Showing RecordList(~p) H(~p) T(~p) ~n", [_RecordList, H, T]),
  {ok, Key} = storeRecord( _C, _NS, _Set, Head, 0),
  storeList( _C, _NS, _Set, Tail,  lists:append(_KeyList, [Key]) ).

%% -----------------------------------------------------------------------
%% Recursively print the Key List
%% -----------------------------------------------------------------------
printKeyList( [] ) -> io:format("PRINT Key List:  All Done~n");

printKeyList( _KeyList ) ->
  [H | T ] = _KeyList,
  io:format("PRINT Key:(~p)  ~n", [H] ),
% io:format("PRINT WHOLE KeyList: Head(~p) Tail(~p) ~n", [H, T] ),
  printKeyList( T ).

%% ------------------------------------------------------------------------
%% fetchRecords that match a KeyList 
%% Usage: {ok, ReturnRecordList} =
%%  fetchRecords( Connect, NameSpace, Set, KeyList, RecordList  ),
%% ------------------------------------------------------------------------
% When the KeyList is empty, then return the accumulated RecordList
fetchRecords( _C, _NS, _Set, [], _RecordList ) ->
  io:format("Returning final RecordList(~p)~n", [_RecordList] ),
  {ok, _RecordList};

%% If there are more keys in the list, fetch the record that corresponds
%% to the Key in the head and process the tail recursively
fetchRecords( _C, _NS, _Set, _KeyList, _RecordList ) ->
  [KeyHead | KeyTail ] = _KeyList,
% io:format("Showing KeyList(~p) ~n RecordList(~p) H(~p) T(~p) ~n",
%   [_KeyList, _RecordList, KeyHead, KeyTail]),
  GetResults = aerospike:getAll(_C, _NS, _Set, KeyHead, 0),
% io:format("Showing Full GetResults(~p)  ~n",[GetResults]),
  [GetHead | _ ] = GetResults,
  {BinName, BinaryValue} = GetHead,
  OriginalValue = binary_to_term( BinaryValue ),
% io:format("FetchRecord: Key(~p) OriginalValue(~p) ~n",
%   [KeyHead, OriginalValue ] ),

  %% Recursively call with remaining "KeyTail" keylist, adding to the
  %% accumulated RecordList
  fetchRecords( _C, _NS, _Set, KeyTail, 
    lists:append(_RecordList, [{BinName,OriginalValue}]) ).

%% ------------------------------------------------------------------------
%% deleteRecords that match a KeyList 
%% Usage: Result = deleteRecords( Connect, NameSpace, Set, KeyList, CLWP ) 
%% ------------------------------------------------------------------------
% When the KeyList is empty, then print "finish message" and return.
deleteRecords( _C, _NS, _Set, [], _CLWP ) ->
  io:format("All Done Deleting Records~n" );

%% If there are more keys in the list, delete the record that corresponds
%% to the Key in the keyList head and process the tail recursively
deleteRecords( _C, _NS, _Set, _KeyList, _CLWP ) ->
  [KeyHead | KeyTail ] = _KeyList,
  DeleteResult = aerospike:delete(_C, _NS, _Set, KeyHead, _CLWP ),
  io:format("Delete: Key(~p) Result(~p)  ~n",[KeyHead, DeleteResult]),

  %% Recursively call with remaining "KeyTail" keylist.
  deleteRecords( _C, _NS, _Set, KeyTail, _CLWP ).

%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% updateRecords( RecordList )
%% Usage: {ok, UpdatedRecordList} =
%%   updateRecords(Connect, NameSpace, Set, ReturnRecordList ),
%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%-
%% -----------------------------------------------------------------------
%% Update Record (add an element to the notes)
%% Usage: UpdatedRecord = RecordUpdate( OldRecord )
%% -----------------------------------------------------------------------
recordUpdate( _OldRecord  ) ->
% io:format("UPDATING RECORD: OldRecord(~p) ~n", [_OldRecord]),
  NewNotes = [{"Really Likes", "Erlang"},{"Aerospike","Is Incredible"}],
  OldNotes = _OldRecord#addressRecord.notes,
  NewList = OldNotes ++  NewNotes,
  NewRecord = _OldRecord#addressRecord{notes = NewList },
% io:format("RECORD UPDATE: NewNotes(~p) NewList(~p) NewRecord(~p) ~n",
%   [NewNotes, NewList, NewRecord] ),
  NewRecord.

%% ------------------------------------------------------------------------
% When the RecordList is empty, then return the accumulated UpdatedRecords
updateRecords( _C, _NS, _Set, [], _UpdatedRecords ) ->
  io:format("Done: Returning Updated List (~p)~n", [_UpdatedRecords] ),
  {ok, _UpdatedRecords };

%% ------------------------------------------------------------------------
%% If there are more Records in the list to update, then process the record
%% that is at the head of the list, and recursively call this method to
%% process the tail of the list.
updateRecords( _C, _NS, _Set, _RecordList, _UpdatedRecords ) ->
  [RecordHead | RecordTail ] = _RecordList,
% io:format("Showing RecordList(~p) ~n UpdatedRecords(~p) H(~p) T(~p) ~n",
%   [_RecordList, _UpdatedRecords, RecordHead, RecordTail]),
  %% Peel off the bin name "AddrRec" from the "RecordHead", so that we
  %% have just the actual "addressRecord" object.
  {"AddrRec", OldRecord } = RecordHead,
  UpdatedRecord = recordUpdate( OldRecord ),
% io:format("Original Record(~p) UpdatedRecord(~p) ~n",
%   [RecordHead, UpdatedRecord ]),

  %% Recursively call with remaining "RecordTail" RecordList, adding to the
  %% accumulated UpdatedRecords List
  updateRecords( _C, _NS, _Set, RecordTail,
    lists:append(_UpdatedRecords, [UpdatedRecord]) ).

%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%-
%% Run Write Parameter Experiments()
%% Usage: test0Result =
%%   runWriteParameterExperiments(Connect, NameSpace, Set, UpdatedRecordList ).
%%
%%  Here is the Erlang counterpart of "cl_write_parameters" struct in
%%  the aerospike C API.
%%
%%  name               | possible values   | Meaning
%%  -------------------------------------------------------------------
%%  unique             | true, false       | Disallow multiple identical keys
%%  unique_bin         | true, false       | Disallow multiple identical bins
%%  use_generation     | true, false       | Use Generation Counts
%%  use_generation_gt  | true, false       | Write ok if NewGen > OldGen
%%  use_generation_dup | true, false       | Write ok if NewGen == OldGen
%%  generation         | positive integer  | Set Generation
%%  timeout_ms         | positive integer  | Set Timeout value
%%  record_ttl         | positive integer  | Set Record Time To Live
%%  w_pol              | cl_write_async,   | Set Write Policy:
%%                     | cl_write_oneshot, |
%%                     | cl_write_retry,   |
%%                     | cl_write_assured  |
%% ----------------------------------------------------------------------
%%
%% Only need to set the values that you want to override the default
%%
%% -record(clwp,
%%         {unique, unique_bin, use_generation, use_generation_gt,
%%          use_generation_dup, generation, timeout_ms, record_ttl, w_pol}).
%%
%% ------------------------------------------------------------------------
%% Create some different Write Parameter Records
%% ----------------------------------------------------------------------
%% These examples show "single value" records, but any or all of the values
%% could potentially be set.
%%
%% newWriteParameterRecord/1 Functions
%%
newWriteParameterRecord( unique ) -> 
  #clwp{unique = true};

newWriteParameterRecord( unique_bin ) -> 
  #clwp{unique_bin = true};

newWriteParameterRecord( use_generation ) -> 
  #clwp{use_generation = true};

newWriteParameterRecord( use_generation_gt ) -> 
  #clwp{ use_generation_gt = true};

newWriteParameterRecord( use_generation_dup ) -> 
  #clwp{ use_generation_dup = true};

newWriteParameterRecord( _Variable ) ->
  io:format("newWriteParameterRecord(1): Undefined parameter(~p) ~n",[_Variable]).

%%
%% newWriteParameterRecord/2 Functions
%%
newWriteParameterRecord( generation, _GenSet ) -> 
  #clwp{use_generation    = true,
        generation  = _GenSet };

newWriteParameterRecord( use_generation_dup, _GenSet ) -> 
  #clwp{use_generation    = true,
        generation        = _GenSet,
        use_generation_dup = true};

newWriteParameterRecord( use_generation_gt, _GenSet ) -> 
  #clwp{use_generation    = true,
        generation        = _GenSet,
        use_generation_gt = true};

newWriteParameterRecord( timeout, _Value ) -> 
  #clwp{timeout_ms  = _Value };

newWriteParameterRecord( record_ttl, _Value ) -> 
  #clwp{record_ttl  = _Value };

newWriteParameterRecord( write_policy, _Value ) -> 
  #clwp{w_pol  = _Value };

newWriteParameterRecord( _Variable, _Value ) ->
  io:format("newWriteParameterRecord(2): Undefined parameter(~p:~p) ~n",
    [_Variable, _Value]).

%%
%% The "big guy" ==>  newWriteParameterRecord/9 
%% Note that we are not checking the parms -- we ASSUME (( ASS -> U -> ME)
%% that the caller knows exactly what he's doing and always passes
%% correct values.   
%%
newWriteParameterRecord( _Unique, _UniqueBin, _UseGen, _UseGenGT, _UseGenDup,
                      _GenSet, _TimeOutMS, _RecordTTL, _WPol ) ->
  #clwp{unique=_Unique, unique_bin=_UniqueBin, use_generation=_UseGen,
    use_generation_gt=_UseGenGT, use_generation_dup=_UseGenDup,
    generation=_GenSet, timeout_ms=_TimeOutMS, record_ttl=_RecordTTL,
    w_pol=_WPol}.

%% ----------------------------------------------------------------------
%% Run numerous experiments, using most of the records and all of the
%% Write Parameters (Write Parameter Settings)
%% ----------------------------------------------------------------------
runWriteParameterExperiments( _C, _NS, _Set, _UpdatedRecordList ) ->
 
  % ----------------------------------------------------------------------
  % Test 1:  Set unique and reinsert a record (same key, updated record)
  % This should throw an error.
  % ----------------------------------------------------------------------
  io:format(">>>>> Start UNIQUE Test(1): Key Unique <<<<< ~n"),
  WriteUnique = newWriteParameterRecord( unique ),
  [ R1 | T1 ] = _UpdatedRecordList,
% io:format("List Head(~p) WriteParameter(~p) ~n", [H1, WriteUnique] ),

  %% Remove Record 1 -- then write it twice.
  DelKey = extractKey( R1 ),
  Response_1_1 = aerospike:delete( _C, _NS, _Set, DelKey, 0 ),
  io:format("Delete: Key(~p) Response(~p) ~n",[DelKey, Response_1_1]),

  % Store Record will extract the key, serialize the record, store the
  % record (with a aerospike:put() ), and return with {Response, Key}.
  io:format("Store Record: Key(~p). Should succeed. ~n",[DelKey]),
  {Response1, K1}  = storeRecord(_C, _NS, _Set, R1, WriteUnique),
  io:format("UNIQUE Test(1): Response(~p) Key(~p) ~n", [Response1,K1]),

  [ R2 | T2 ] = T1, %% Get the next record in the list and store with same key
  io:format("Store Record: Key(~p). Should fail. ~n",[DelKey]),
  {Response11, K11}  = storeRecord(_C, _NS, _Set, K1, R2, WriteUnique),
  io:format("UNIQUE Test(1a): Response(~p) Key(~p) ~n", [Response11,K11]),

% %% fetchRecords(ConnectId, Namespace, Set, KeyList, ResultList)
% FetchResult1 = fetchRecords( _C, _NS, _Set, [K1], [] ),
% io:format("Fetching Record(s) for Key(~p): ~p ~n",[K1, FetchResult1]),
  io:format("<><><><> End UNIQUE Test(1): <><><><> ~n"),

  % ----------------------------------------------------------------------
  % Test 2:  Set unique_bin and reinsert a record (same key, updated record)
  % ----------------------------------------------------------------------
  io:format(">>>>> Start UNIQUE Bin Test(2): Bin Unique <<<<< ~n"),
  WriteBinUnique = newWriteParameterRecord( unique_bin ),
  % Store Record will extract the key, serialize the record, store the
  % record (with a aerospike:put() ), and return with {Response, Key}.
  io:format("Unique Bin Test(2): Expect Failure: Non-Unique Bin ~n" ),
  {Response2, K2}  = storeRecord(_C, _NS, _Set, R1, WriteBinUnique),
  io:format("UNIQUE Bin Test(2): Response(~p) Key(~p) ~n", [Response2,K2]),
  io:format("<><><><> End UNIQUE Bin Test(2): <><><><> ~n"),

  % ----------------------------------------------------------------------
  % Test 3: Use Generation Counts:
  % (*) Generation Counts are always managed by the server, and they always
  %     start at ZERO.
  % (*) Generation Counts are incremented every time the record is modified.
  % (*) We have three different types of tests that we can perform w.r.t.
  %     generation counts.
  % (*) Once generation tracking is turned on, it becomes an attribute.
  %     However, we don't return the Gen Count currently in the Erlang
  %     get() call.  Bummer.

  % Remove a record so that we can reuse it -- then write it once with
  % a new generation number -- then write it again with a different
  % generation number -- and see the error.
  % ----------------------------------------------------------------------
  io:format(">>>>> Start Generation Test(3): Gen Dup <<<<< ~n"),
  Response_3_1 = aerospike:delete( _C, _NS, _Set, K1, 0 ),
  io:format("GenTest(3): Delete Response(~p) ~n", [Response_3_1] ),

  % Write the record again -- NEW.  Gen ZERO.  Expect success.
  WP_30 = newWriteParameterRecord( generation, 0),
  Response_3_2 = storeRecord(_C, _NS, _Set, R1, WP_30  ),
  io:format("GenTest(3a):Store(Expect OK) K(~p) Response(~p) ~n",
    [K1, Response_3_2] ),

  %% The correct Generation Number here would be ONE, not ZERO. Expect Fail
  WP_31 = newWriteParameterRecord( use_generation_dup, 0 ),
  Response_3_3 = storeRecord(_C, _NS, _Set, R1, WP_31 ),
  io:format("GenTest(3b):Store:(Expect Fail) K(~p) Response(~p)~n",
    [K1, Response_3_3] ),

  %% Try with the correct Generation Number(ONE).  Expect Success.
  WP_32 = newWriteParameterRecord( use_generation_dup, 1 ),
  Response_3_4 = storeRecord(_C, _NS, _Set, R1, WP_32 ),
  io:format("GenTest(3c):Store:(Expect OK) K(~p) Response(~p)~n",
    [K1, Response_3_4] ),

  io:format("<><><><> End Generation Test(3): <><><><> ~n"),


% % ----------------------------------------------------------------------
% % Test 4: Generation: Test GREATER THAN (or equal to) what's on the server
% % ----------------------------------------------------------------------
  io:format(">>>>> Start Generation Test(4): Gen GT <<<<< ~n"),
  Response_4_1 = aerospike:delete( _C, _NS, _Set, K1, 0 ),
  io:format("Generation Test(4): Delete Response(~p) ~n", [Response_4_1] ),

  % Write the record again -- NEW.  Gen ZERO.  Expect success.
  WP_40 = newWriteParameterRecord( generation, 0),
  Response_4_2 = storeRecord(_C, _NS, _Set, R1, WP_40 ),
  io:format("GenTest(4a): Store(Expect OK) K(~p) Response(~p) ~n",
    [K1, Response_4_2] ),

  %% The correct Generation Number here would be ONE, not ZERO. Expect Fail
  WP_41 = newWriteParameterRecord( use_generation_gt, 0 ),
  Response_4_3 = storeRecord(_C, _NS, _Set, R1, WP_41 ),
  io:format("GenTest(4b): Store(0:Expect Fail) Key(~p) Store(~p) ~n",
    [K1, Response_4_3] ),

  %% Try with the correct Generation Number(ONE).  Expect Success.
  WP_42 = newWriteParameterRecord( use_generation_gt, 1 ),
  Response_4_4 = storeRecord(_C, _NS, _Set, R1, WP_42 ),
  io:format("GenTest(4c): Store(1:Expect OK) K(~p) Response(~p)~n",
    [K1, Response_4_4] ),

  %% Try with the correct Generation Number(TWO).  Expect Success.
  WP_43 = newWriteParameterRecord( use_generation_gt, 2 ),
  Response_4_5 = storeRecord(_C, _NS, _Set, R1, WP_43 ),
  io:format("GenTest(4d): Store(2:Expect OK) K(~p) Response(~p)~n",
    [K1, Response_4_5] ),

  io:format("<><><><> End Generation Test(4): <><><><> ~n"),

% % ----------------------------------------------------------------------
% % Test 5: We will test the Write options, but the result will
%   not be visible here -- it's a performance difference, not something
%   we could see from here.  However, we'll show the settings as an
%   example.
% % ----------------------------------------------------------------------
%%  w_pol =
%%  cl_write_async | cl_write_oneshot | cl_write_retry | cl_write_assured
  io:format(">>>>> Start WriteParameter Test(5): <<<<< ~n"),

  % Write A NEW RECORD -- try policy cl_write_async
  WP_50 = newWriteParameterRecord( w_pol, cl_write_async ),
  Key50 = "WP_Test_50",
  Record50 = [{"WP_Test_bin", "WP_Test_Value_50"}],
  Response_50 = aerospike:put(_C, _NS, _Set, Key50, Record50, WP_50),
  io:format("WP(5a): Store(Expect OK) K(~p) Response(~p) ~n",
    [Key50, Response_50] ),

  % Write A NEW RECORD -- try policy cl_write_oneshot
  WP_51 = newWriteParameterRecord( w_pol, cl_write_oneshot ),
  Key51 = "WP_Test_51",
  Record51 = [{"WP_Test_bin", "WP_Test_Value_51"}],
  Response_51 = aerospike:put(_C, _NS, _Set, Key51, Record51, WP_51),
  io:format("WP(5a): Store(Expect OK) K(~p) Response(~p) ~n",
    [Key51, Response_51] ),

  % Write A NEW RECORD -- try policy cl_write_retry
  WP_52 = newWriteParameterRecord( w_pol, cl_write_retry ),
  Key52 = "WP_Test_52",
  Record52 = [{"WP_Test_bin", "WP_Test_Value_52"}],
  Response_52 = aerospike:put(_C, _NS, _Set, Key52, Record52, WP_52),
  io:format("WP(5a): Store(Expect OK) K(~p) Response(~p) ~n",
    [Key52, Response_52] ),

  % Write A NEW RECORD -- try policy cl_write_assured
  WP_53 = newWriteParameterRecord( w_pol, cl_write_assured ),
  Key53 = "WP_Test_53",
  Record53 = [{"WP_Test_bin", "WP_Test_Value_53"}],
  Response_53 = aerospike:put(_C, _NS, _Set, Key53, Record53, WP_53),
  io:format("WP(5a): Store(Expect OK) K(~p) Response(~p) ~n",
    [Key53, Response_53] ),

  io:format("<><><><> End Write Policy  Test(5): <><><><> ~n"),

  %% Return the keys we've created, so they can be deleted in cleanup.
  [Key50, Key51, Key52, Key53].

%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Canned Tests that we can just invoke once from the command line.
%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% ------------------------------------------------------------------------
%% Test 0: 
%% Manage Records in the Aerospike Erlang Client.
%% Create Records, flatten them (into binary), store them, retrieve them,
%% Modify them re-write them -- exploiting some of the Aerospike Write
%% Parameter settings (e.g. Unique, Generation, Timeout, etc).
%%
%% (*) Connect to a cluster (probably local host)
%% (*) Create some addressRecord Records
%% (*) Store Records, returning the list of stored keys
%%     Note that Keys are extracted from the record using Name+cellPhone
%% (*) Fetch Records
%% (*) Update Records
%% (*) Experiment with Write Policy Parameters
%% ------------------------------------------------------------------------
%% Usage: TestResult = test0().
%% ------------------------------------------------------------------------
test0() ->
  %% Set the parms
  TestNumber = 0,
  Host = "127.0.0.1",  %% Assume a server is installed and running locally
                       %% ==>  /etc/init.d/citrusleaf start
  Port = 3000,
  NS = "test",
  Set = "test_set",
  io:format(" <><><> Record Test (~w) <><><> ~n", [TestNumber]),
  io:format(" ::Host(~s) Port(~w) NameSpace(~s) Set(~s) ~n",
    [Host, Port, NS, Set ] ),
  {ok, C } = aerospike:connect( Host, Port ),

  {ok, RecordList} = populateRecordList(),
  io:format("Results of populateRecordList(): ~p ~n", [RecordList]),

  {ok, KeyList} = serializeAndStoreRecords( C , NS, Set, RecordList ), 
% printKeyList( KeyList ),

  {ok, ReturnRecordList} = fetchRecords( C , NS, Set, KeyList, []),
  io:format("Results of fetchRecords(): ~p ~n", [ReturnRecordList]),

  {ok, UpdatedRecordList} = updateRecords( C , NS, Set, ReturnRecordList, []),
  io:format("Results of updateRecords(): ~p ~n", [UpdatedRecordList]),

  io:format("WP<begin>PWPWP << Write Parameter Tests >> WPWPWPWPWPWPWPWP ~n"),
  ExtraKeyList = runWriteParameterExperiments( C , NS, Set, UpdatedRecordList ),
  io:format("WPW<end>WPWPWP << Write Parameter Tests >> WPWPWPWPWPWPWPWP ~n"),

  io:format("DD<begin>DDDDDDD << Delete All Records >> DDDDDDDDDDDDDDDD ~n"),
  deleteRecords( C, NS, Set, KeyList ++ ExtraKeyList, 0),
  io:format("DDD<end>DDDDDDDD << Delete All Records >> DDDDDDDDDDDDDDDD ~n"),

  aerospike:shutdownAll(),
  io:format("<!><!><!><!><!> End of Test0 <!><!><!><!><!> ~n" ).

%% ------------------------------------------------------------------------
%% <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF>
%% ------------------------------------------------------------------------
