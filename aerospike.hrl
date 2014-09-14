%%% Copyright (C) 2013  by Aerospike.  All rights reserved.  
%%
%% clwp: (Citrusleaf Write Parameters)
%%  The clwp record is the Erlang counterpart of "cl_write_parameters"
%%  struct in the Aerospike C API.
%%
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
%%  w_pol              | cl_write_async,   | Write Policy:
%%                     | cl_write_oneshot, |
%%                     | cl_write_retry,   |
%%                     | cl_write_assured  |
%% ----------------------------------------------------------------------
%% Only need to set the values that you want to override the default
%%
-record(clwp, {unique, unique_bin, use_generation, use_generation_gt,
        use_generation_dup, generation, timeout_ms, record_ttl, w_pol}).
