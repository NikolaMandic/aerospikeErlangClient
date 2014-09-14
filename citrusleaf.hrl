%% clwp:
%%    clwp here is the Erlang counterpart of "cl_write_parameters" struct in
%%	the citrusleaf C API.
%%
%%            name               |       possible values
%%         -----------------------------------------------
%%          unique               |     true, false
%%          unique_bin           |     true, false
%%          use_generation       |     true, false
%%          use_generation_gt    |     true, false
%%          use_generation_dup   |     true, false
%%          generation           |     positive integer
%%          timeout_ms           |     positive integer
%%          record_ttl           |     positive integer
%%          w_pol                |     cl_write_async, cl_write_oneshot,
%%					cl_write_retry, cl_write_assured
%%
%% Only need to set the values that you want to override the default
%%
-record(clwp, {unique, unique_bin, use_generation, use_generation_gt,
        use_generation_dup, generation, timeout_ms, record_ttl, w_pol}).


%% -------------------------------------------------------------------------
%% DataType: stats
%% - read_ok:    Count of Read Operations that have succeeded
%% - read_err:   Count of Read Operations that have failed
%% - write_ok:   Count of Write Operations that have succeeded
%% - write_err:  Count of Write Operations that have failed
%% -------------------------------------------------------------------------
-record(stats,
  { read_ok = 0,
    read_err = 0,
    write_ok = 0,
    write_err = 0 }).

