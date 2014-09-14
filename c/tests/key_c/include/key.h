/*
 *  Citrusleaf Key Test
 *  An example program using the C interface
 *  include/key.h
 *
 *  Copyright 2009 by Citrusleaf.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 *
 * Brian Bulkowski
 */
#pragma once

#include <pthread.h>
#include <stdint.h>
#include "citrusleaf/citrusleaf.h"

extern uint64_t rand_64();

extern int do_key_test ( uint n_keys, uint n_reads, uint n_writes, uint n_deletes,  uint n_threads, uint key_len, uint value_len, uint n_ops, uint writeratio, bool parallel);

typedef struct config_s {
	
	char *host;
	int   port;
	char *ns;
	char *table;
	char *bin;
	bool verbose;
	bool silent; // the opposite of verbose
	bool host_direct;
	bool continuous;
	bool server_validate;
	bool parallel;
	int	 n_batch; // 0 is standard reads
	uint32_t timeout;
	uint32_t record_ttl; // number of seconds before expiration starts
	uint32_t ttl_range; // spread TTLs from record_ttl to record_ttl + ttl_range

	int zero; 			// where to start in the keyspace (allows two clients to insert simultaneously) 
	int value_factor;   // value is key * value factor, important for testing consistancy of writes
	
	
	int value_type; // a CL type - integer or string or blob
	int key_type; // a CL type - integer or string
	
	uint threads;
	
	uint delay; // a funky parameter
	            // if < 1000, insert a 'yeild' after some transactions
	bool read_modify_write;
	bool use_shm;
	
} config;

extern config g_config;

typedef struct atomic_int_s {
	uint32_t		val;
	pthread_mutex_t	lock;
} atomic_int;

extern atomic_int	*atomic_int_create(uint32_t val);
extern void			atomic_int_destroy(atomic_int *ai);
extern uint32_t		atomic_int_add(atomic_int *ai, uint32_t val);
extern uint32_t		atomic_int_get(atomic_int *ai);


extern void *start_counter_thread(char *msg, atomic_int *keys, atomic_int *gen_errs);
extern void stop_counter_thread(void *id);

//
// Write ops are requested by the number of keys to add, and the
// starting key. These will be algorithmically constructed

typedef struct {

	uint start_key;
	uint n_writes;
	uint key_len, value_len;

	int 	id;
	
	atomic_int *completed;
	atomic_int *gen_errs;
	
	cl_cluster *asc;
	
} write_work;

typedef struct {

	uint start_key;
	uint n_deletes;
	uint key_len, value_len;

	int     id;

	atomic_int *completed;
	atomic_int *gen_errs;

	cl_cluster *asc;

} delete_work;

typedef struct {
	
	uint n_reads;
	uint start_key;      // starting at this key
	uint n_keys; 		// randomize up to this number
	uint key_len, value_len;
	
	int id;

	atomic_int *completed;	
	cl_cluster *asc;
} read_work;

typedef struct {

	uint start_key;
	uint n_ops;
	uint n_keys; 		// randomize up to this number
	uint key_len, value_len;

	int 	id;
	
	atomic_int *rcompleted;
	atomic_int *wcompleted;
	int	writeratio;
	atomic_int *gen_errs;
	
	cl_cluster *asc;
	
} op_work;


