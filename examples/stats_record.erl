%%% Copyright (C) 2013  by Citrusleaf.  All rights reserved.  
%%% THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
%%%  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.        
%%
%% This file holds the record structure and functions for managing
%% the benchmark statistics.
%% So far, we are just tracking the Success and Failures of Reads and Writes
%%  This file accomipanies:
%% (*) asbench.erl
%% (*) citrusleaf.erl
%% 
%% 1/21/2013 tjl:
%%

-module(stats_record).
-compile(export_all).
-include("citrusleaf.hrl").
-include("asbench.hrl").

makeRecord( _SeqNum) ->
  #stats{ read_ok = 0, read_err = 0, write_ok = 0, write_err = 0,
          process_num = _SeqNum, general_ok = 0, general_err = 0 }.

incrGeneralOk( _SR ) ->
% io:format("Incrementing  GENERAL OK  ~n"),
  _SR#stats{general_ok = _SR#stats.general_ok + 1 }.

incrGeneralErr( _SR ) ->
% io:format("Incrementing  GENERAL ERROR  ~n"),
  _SR#stats{general_err = _SR#stats.general_err + 1 }.

incrReadOk( _SR ) ->
% io:format("Incrementing READ OK ~n"),
  _SR#stats{read_ok = _SR#stats.read_ok + 1 }.

incrReadErr( _SR ) ->
% io:format("Incrementing READ ERROR ~n"),
  _SR#stats{read_err = _SR#stats.read_err + 1 }.

incrWriteOk( _SR ) ->
% io:format("Incrementing WRITE OK ~n"),
  _SR#stats{write_ok = _SR#stats.write_ok + 1 }.

incrWriteErr( _SR ) ->
% io:format("Incrementing WRITE ERROR ~n"),
  _SR#stats{write_err = _SR#stats.write_err + 1 }.

showStats( _SR ) ->
  io:format("~n>> Stats Report:: Read Success(~p) Read Failures(~p) ",
    [_SR#stats.read_ok, _SR#stats.read_err]),
  io:format(">> :: Write Success(~p) Write Failures(~p) ~n ",
    [_SR#stats.write_ok, _SR#stats.write_err ]),
  io:format(">> :: General Success(~p) General Err(~p) ~n ",
    [ _SR#stats.general_ok, _SR#stats.general_err ]).
