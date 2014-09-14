%% File asbench.hrl
%% -------------------------------------------------------------------------
%% Use this record for tracking progress (success and error status) during
%% the benchmark run.
%%
%% DataType: stats
%% - Process Num: ID of the process (sequence number, etc)
%% - read_ok:    Count of Read Operations that have succeeded
%% - read_err:   Count of Read Operations that have failed
%% - write_ok:   Count of Write Operations that have succeeded
%% - write_err:  Count of Write Operations that have failed
%% - general_ok:  Count of other successes (Things not yet tracked)
%% - general_err:  Count of other errors (Things not yet tracked)
%% -------------------------------------------------------------------------
-record(stats, {
    process_num = 0,
    read_ok = 0,
    read_err = 0,
    write_ok = 0,
    write_err = 0,
    general_ok = 0,
    general_err = 0 }).

