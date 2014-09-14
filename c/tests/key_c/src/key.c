/*
 *  Citrusleaf Tests
 *  src/key.c - Key-value oriented test code
 *
 *  Copyright 2008 by Citrusleaf.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdbool.h>
#include <pthread.h>
#include <signal.h>

#include "key.h"
#include "histogram.h"
#include "citrusleaf/citrusleaf.h"

static histogram* g_p_read_histogram;
static histogram* g_p_write_histogram;
static histogram* g_p_delete_histogram;
static histogram* g_p_read_timeout_histogram;
static histogram* g_p_write_timeout_histogram;
static histogram* g_p_delete_timeout_histogram;


// helper
static inline uint64_t safe_delta_ms(uint64_t start_ms, uint64_t stop_ms) {
	return start_ms > stop_ms ? 0 : stop_ms - start_ms;
}

static void
dump_buf(char *msg, uint8_t *buf, size_t buf_len)
{
	fprintf(stderr, "dump_buf: %s\n",msg);
	uint i;
	for (i=0;i<buf_len;i++) {
		if (i % 16 == 8)
			fprintf(stderr, " : ");
		if (i && (i % 16 == 0))
			fprintf(stderr, "\n");
		fprintf(stderr, "%02x ",buf[i]);
	}
	fprintf(stderr, "\n");
}



// #define DEBUG 1
// #define DEBUG_VERBOSE 1
// #define PRINT_KEY 1

typedef struct key_value_s {
	
	uint64_t key_int;
	
	char 	*key_str;
	
	char *value_str; // this points to the area in memory allocated to the key_value (no free)
	
	int   value_blob_len;
	void *value_blob; // this points to the area in memory allocated to the key_value (no free)
	
	uint64_t value_int;
	
} key_value;


// Make a key and value
// When you're finished with it, just free the structure - the following strings
// are allocated as part of the structure
//
// Set up both value and integer ---
// 
static key_value *make_key_value(uint seed, uint key_len, uint value_len)
{
	int i;
	size_t	sz = sizeof(key_value) + key_len + value_len + 2 + value_len;
	key_value *kv = malloc(sz);
	if (!kv)	return(0);

	// save before molesting
	kv->key_int = kv->value_int = seed * g_config.value_factor;
	
	// a little hinky, pointing internally to local memory, but so much more efficient
	kv->key_str = ((char *)kv)+ sizeof(key_value);
	kv->value_str = ((char *)kv)+ sizeof(key_value) + key_len + 1;
	kv->value_blob = kv->value_str + value_len + 1;
	kv->value_blob_len = value_len;
	
	// generate the string key
	kv->key_str[key_len] = 0;
	for (i = key_len-1; i >= 0; i--) {
		kv->key_str[i] = (seed % 10) + '0';
		seed = seed / 10;
	}
	
	// build a string out of the value, might be different from key
	// (optimization: don't build if integer test, or if value_factor == 1
	char value_str_short[key_len];
	seed = kv->value_int;
	value_str_short[key_len] = 0;
	for (i = key_len-1; i >= 0; i--) {
		value_str_short[i] = (seed % 10) + '0';
		seed = seed / 10;
	}
	
	
	// longer value likely required: multiple copies of value_str_short
	i=0;
	while (i < value_len) {
		if (value_len - i > key_len) {
			memcpy(&kv->value_str[i], value_str_short, key_len);
			i += key_len;
		}
		else {
			memcpy(&kv->value_str[i], value_str_short, value_len - i);
			i += (value_len - i);
		}
	}	
	kv->value_str[value_len] = 0;

	// And how's about a fine blob!
	uint64_t t = kv->value_int;
	memcpy(kv->value_blob, &t, sizeof(t)); // this will make sure some nulls, always a good idea
	memcpy(kv->value_blob + sizeof(t), kv->value_str,kv->value_blob_len - sizeof(t));

	
#ifdef DEBUG_VERBOSE
	fprintf(stderr, "make key value: key_len %d key %s value_len %d value_str %s value_int %"PRIu64"\n",key_len,kv->key,value_len,kv->value_str,kv->value_int);
#endif
	
	return(kv);	
}



void *
read_fn(void *udata)
{
	read_work *work = (read_work *) udata;
	uint32_t delay_factor = 0;

	if (g_config.verbose) fprintf(stderr, " read thread started: key_start %d nkeys %d\n",work->start_key,work->n_keys);
	
	for (uint i=0;i<work->n_reads;i++) {
	
		cl_rv rv;
		uint key_int = (rand_64() % work->n_keys) + work->start_key;
		key_value *kv = make_key_value(key_int, work->key_len, work->value_len);

		// Create the key we want to read
		cl_object o_key;
		if (g_config.key_type == CL_STR) {
			citrusleaf_object_init_str(&o_key,kv->key_str);
		}
		else if (g_config.key_type == CL_INT) {
			citrusleaf_object_init_int(&o_key,kv->key_int);
		}
		else {
			if (!g_config.silent) fprintf(stderr, " read thread: unsupport key type %d\n",g_config.key_type);
		}
	
		// bundle it into a value array
		cl_bin values[1];
		memset(values, 0, sizeof(values));
		strcpy(values[0].bin_name, g_config.bin);
	
#ifdef PRINT_KEY
		cf_digest d;
		citrusleaf_calculate_digest(g_config.table, &o_key, &d);
		fprintf(stderr, "get: key %d digest %"PRIx64"\n",key_int, *(uint64_t *)&d);
#endif
		
		
		if (g_config.server_validate) {
	
			if (g_config.value_type == CL_STR) {
				citrusleaf_object_init_str(&values[0].object, kv->value_str);
			}
			else if (g_config.value_type == CL_BLOB) {
				citrusleaf_object_init_blob(&values[0].object, kv->value_blob, kv->value_blob_len);
			}
			else if (g_config.value_type == CL_INT) {
				citrusleaf_object_init_int(&values[0].object, kv->value_int);
			}
			else {
				if (!g_config.silent) fprintf(stderr, "INTERNAL ERROR: unexpected value type under verify, help!\n");
			}
			if (CITRUSLEAF_OK != (rv = citrusleaf_verify(work->asc, g_config.ns, g_config.table, &o_key,values, 1, g_config.timeout, NULL) ) ) 
			{
				if (!g_config.silent) fprintf(stderr, "read: validate: key %d value %"PRIu64" failed with %d\n",key_int,kv->value_int, rv);
				citrusleaf_object_free(&values[0].object);
				if (g_config.continuous == false) {
					free(kv);
					return((void *)-1);
				}
				goto NextRead;
			}
		}

		else {
			// do the actual work of reading
			uint64_t start_time = cf_getms();
			if (CITRUSLEAF_OK != (rv = citrusleaf_get(work->asc, g_config.ns, g_config.table, &o_key, values, 1, g_config.timeout, 0))) {
	            uint64_t stop_time = cf_getms();
	            uint64_t diff = safe_delta_ms(start_time, stop_time);

	        	if (rv == CITRUSLEAF_FAIL_TIMEOUT || rv == CITRUSLEAF_FAIL_SERVERSIDE_TIMEOUT) {
	                histogram_insert_data_point(g_p_read_timeout_histogram, diff);

	                if (!g_config.silent) {
                        fprintf(stderr, "citrusleaf get timeout %d %"PRIu64" %d\n",rv,diff,g_config.timeout);
	                }
	        	}
	        	else {
	                if (!g_config.silent) {
	                	fprintf(stderr, "read: failed with error %d\n",rv);
	                }
	        	}
				citrusleaf_object_free(&values[0].object);
				if (g_config.continuous == false) {
					free(kv);
					return((void *)-1);
				}
				goto NextRead;
			}
			uint64_t stop_time = cf_getms();
 			histogram_insert_data_point(g_p_read_histogram,
			safe_delta_ms(start_time, stop_time));
		}
		
		// validate
		
		
		if (values[0].object.type != g_config.value_type) {
			if (!g_config.silent) fprintf(stderr, "read value has wrong type: expect %d got %d\n",(int) g_config.value_type, (int) values[0].object.type);
			if (g_config.continuous == false) {
				citrusleaf_object_free(&values[0].object);
				free(kv);
				return((void *)-1);
			}
			raise(SIGINT);
			goto NextRead;
		}
		
		if (g_config.value_type == CL_STR) {
			if (values[0].object.u.str == 0) {
				if (!g_config.silent) fprintf(stderr, "read value pointer is null. Yuck.\n");
				citrusleaf_object_free(&values[0].object);
				if (g_config.continuous == false) {
					free(kv);
					return((void *)-1);
				}
				goto NextRead;
			}
						
			if (values[0].object.sz  != strlen(kv->value_str)) {
				if (!g_config.silent) fprintf(stderr, "read value size doesn't match expected. Is %zd expecting %zd\n",
					values[0].object.sz,strlen(kv->value_str)+1);
				citrusleaf_object_free(&values[0].object);
				if (g_config.continuous == false) {
					free(kv);					
					return((void *)-1);
				}
				goto NextRead;
			}			
			
			if (strcmp(values[0].object.u.str, kv->value_str) != 0) {
				if (!g_config.silent) {
					fprintf(stderr, "read value does not match set value.\n");
					fprintf(stderr, "  expecting: %s\n",kv->value_str);
					fprintf(stderr, "  got:       %s\n",values[0].object.u.str);
				}
				citrusleaf_object_free(&values[0].object);

				if (g_config.continuous == false) {
					free(kv);
					return((void *)-1);
				}
				goto NextRead;
			}
		}
		else if (g_config.value_type == CL_BLOB) {
			if (values[0].object.u.blob == 0) {
				if (!g_config.silent) fprintf(stderr, "read value pointer is null. Yuck.\n");
				citrusleaf_object_free(&values[0].object);
				if (g_config.continuous == false) {
					free(kv);
					return((void *)-1);
				}
				goto NextRead;
			}
			
			if (values[0].object.sz  != kv->value_blob_len) {
				if (!g_config.silent) fprintf(stderr, "read value size doesn't match expected. Is %zd expecting %d\n",
					values[0].object.sz,kv->value_blob_len);
				citrusleaf_object_free(&values[0].object);

				if (g_config.continuous == false) {
					free(kv);					
					return((void *)-1);
				}
				goto NextRead;
			}			
			
			if (memcmp(values[0].object.u.blob, kv->value_blob, kv->value_blob_len) != 0) {
				if (!g_config.silent) {
					int k;
					uint8_t *b1 = (uint8_t *) values[0].object.u.blob;
					uint8_t *b2 = (uint8_t *) kv->value_blob;
					for (k=0;k<kv->value_blob_len;k++) {
						if (b1[k] != b2[k]) break;
					}
					
					fprintf(stderr, "read value does not match set value, key int %d, differ at byte %d (length correct).\n",key_int,k);
					dump_buf("got buf: ", b1, kv->value_blob_len);
					dump_buf("expecting buf: ", b2, kv->value_blob_len);

				}
				citrusleaf_object_free(&values[0].object);

				if (g_config.continuous == false) {
					free(kv);
					return((void *)-1);
				}
				goto NextRead;
			}

			
		}
		else if (g_config.value_type == CL_INT) {
			
			if (values[0].object.u.i64 != kv->value_int) {
				if (!g_config.silent) {
					fprintf(stderr, "read value does not match set value.\n");
					fprintf(stderr, "  expecting: %"PRIu64"\n",kv->value_int);
					fprintf(stderr, "  got:       %"PRIu64"\n",values[0].object.u.i64);
				}
				citrusleaf_object_free(&values[0].object);
				if (g_config.continuous == false) {
					free(kv);
					return((void *)-1);
				}
				goto NextRead;

			}			
			
		}

		// have to hand back any possible malloced memory
		citrusleaf_object_free(&values[0].object);
NextRead:
		free(kv);
		
		atomic_int_add(work->completed, 1);
		
		if (g_config.delay) {
			if (g_config.delay >= 1000) {
				usleep( (g_config.delay - 1000) * 1000 );
			}
			else {
				if ( (delay_factor++ % (1000 - g_config.delay)) == 0) {
					usleep( 1000 );
				}
			}
		}
		
	}
	return((void *)0);
}


typedef struct {
	cf_digest *digests;
	key_value **kvs;
	uint *key_ints;
	int batch_factor;
	int n_received_success;
	int n_received_error;
}	getmany_userdata;


int my_max(int a, int b)
{
	return (a > b) ? a : b;
}

int my_min(int a, int b)
{
	return (a < b) ? a : b;
}

int
batch_getmany_cb(char *ns, cf_digest *keyd, char *set, uint32_t generation, uint32_t record_ttl, cl_bin *bins, int n_bins,
	bool is_last, void *udata)
{
	getmany_userdata *gm_udata = (getmany_userdata *) udata;
	cf_digest *keys = gm_udata->digests;
	int batch_factor = gm_udata->batch_factor;
	
	// find the kvs * key_int that matches the request we're making
	key_value *kv = 0;
	uint key_int = 0;
	for (int i=0;i<batch_factor;i++) {
		if (0 == memcmp(&keys[i],keyd,sizeof(cf_digest))) {
			kv = gm_udata->kvs[i];
			key_int = gm_udata->key_ints[i];
			break;
		}
	}
	if (kv == 0) {
		if (!g_config.silent) fprintf(stderr, "read batch value: digest does not match digests we are looking for\n");
		gm_udata->n_received_error++; // maybe needs atomic?
		return(-1);
	}
	
	// start validating	
	if (bins[0].object.type != g_config.value_type) {
		if (!g_config.silent) fprintf(stderr, "read value has wrong type: expect %d got %d\n",(int) g_config.value_type, (int) bins[0].object.type);
		gm_udata->n_received_error++; // maybe needs atomic?
		citrusleaf_object_free(&bins[0].object);
		return(-1);
	}
	
	if (g_config.value_type == CL_STR) {
		if (bins[0].object.u.str == 0) {
			gm_udata->n_received_error++; // maybe needs atomic?
			if (!g_config.silent) fprintf(stderr, "read value pointer is null. Yuck.\n");
			citrusleaf_object_free(&bins[0].object);
			return(-1);
		}
					
		if (bins[0].object.sz  != strlen(kv->value_str)) {
			gm_udata->n_received_error++; // maybe needs atomic?
			if (!g_config.silent) fprintf(stderr, "read value size doesn't match expected. Is %zd expecting %zd\n",
				bins[0].object.sz,strlen(kv->value_str)+1);
			citrusleaf_object_free(&bins[0].object);
			return(-1);
		}			
		
		if (strcmp(bins[0].object.u.str, kv->value_str) != 0) {
			gm_udata->n_received_error++; // maybe needs atomic?
			if (!g_config.silent) {
				fprintf(stderr, "read value does not match set value.\n");
				fprintf(stderr, "  expecting: %s\n",kv->value_str);
				fprintf(stderr, "  got:       %s\n",bins[0].object.u.str);
			}
			citrusleaf_object_free(&bins[0].object);
			return(-1);
		}
	}
	else if (g_config.value_type == CL_BLOB) {
		if (bins[0].object.u.blob == 0) {
			gm_udata->n_received_error++; // maybe needs atomic?
			if (!g_config.silent) fprintf(stderr, "read value pointer is null. Yuck.\n");
			citrusleaf_object_free(&bins[0].object);
			return(-1);
		}
		
		if (bins[0].object.sz  != kv->value_blob_len) {
			gm_udata->n_received_error++; // maybe needs atomic?
			if (!g_config.silent) fprintf(stderr, "read value size doesn't match expected. Is %zd expecting %d\n",
				bins[0].object.sz,kv->value_blob_len);
			citrusleaf_object_free(&bins[0].object);
			return(-1);
		}			
		
		if (memcmp(bins[0].object.u.blob, kv->value_blob, kv->value_blob_len) != 0) {
			gm_udata->n_received_error++; // maybe needs atomic?
			if (!g_config.silent) {
				int k;
				uint8_t *b1 = (uint8_t *) bins[0].object.u.blob;
				uint8_t *b2 = (uint8_t *) kv->value_blob;
				for (k=0;k<kv->value_blob_len;k++) {
					if (b1[k] != b2[k]) break;
				}
				
				fprintf(stderr, "read value does not match set value, key int %d, differ at byte %d (length correct).\n",key_int,k);
				dump_buf("got buf: ", b1, kv->value_blob_len);
				dump_buf("expecting buf: ", b2, kv->value_blob_len);

			}
			citrusleaf_object_free(&bins[0].object);
			return(-1);
		}
		
	}
	else if (g_config.value_type == CL_INT) {
		
		if (bins[0].object.u.i64 != kv->value_int) {
			gm_udata->n_received_error++; // maybe needs atomic?
			if (!g_config.silent) {
				fprintf(stderr, "read value does not match set value.\n");
				fprintf(stderr, "  expecting: %"PRIu64"\n",kv->value_int);
				fprintf(stderr, "  got:       %"PRIu64"\n",bins[0].object.u.i64);
			}
			citrusleaf_object_free(&bins[0].object);
			return(-1);

		}			
		
	}
	
	gm_udata->n_received_success++; // maybe needs atomic?

//	fprintf(stderr, "read value success\n");
//	fprintf(stderr, "  expecting: %"PRIu64"\n",kv->value_int);
//	fprintf(stderr, "  got:       %"PRIu64"\n",bins[0].object.u.i64);

	
	// have to hand back any possible malloced memory
//	citrusleaf_object_free(&values[0].object);
	
	return(0);
}

void *
read_batch_fn(void *udata)
{
	read_work *work = (read_work *) udata;
	uint32_t delay_factor = 0;

	if (g_config.verbose) fprintf(stderr, " read thread started: key_start %d nkeys %d\n",work->start_key,work->n_keys);
	
	cf_digest *digests = alloca( sizeof(cf_digest) * g_config.n_batch );
	if (!digests) return(0);
	
	key_value **kvs = alloca( sizeof(key_value *) * g_config.n_batch);
	if (!kvs) return(0);
	
	uint *key_ints = alloca( sizeof(uint) * g_config.n_batch );
	if (!key_ints) return(0);

	getmany_userdata gm_udata;
	gm_udata.digests = digests;
	gm_udata.kvs = kvs;
	gm_udata.key_ints = key_ints;
	gm_udata.batch_factor = g_config.n_batch;
	
	// copy the bin name we want to fetch
	cl_bin values[1];
	memset(values, 0, sizeof(values));
	strcpy(values[0].bin_name, g_config.bin);

	cl_rv rv;
	
	// grab an array of digests to fetch
	for (uint i=0;i<work->n_reads / g_config.n_batch;i++) {
	
		for (int j=0; j < g_config.n_batch; j++) {
		
			uint key_int = key_ints[j] = (rand_64() % work->n_keys) + work->start_key;
			key_value *kv = kvs[j] = make_key_value(key_int, work->key_len, work->value_len);
	
			// Create the key we want to read
			cl_object o_key;
			if (g_config.key_type == CL_STR) {
				citrusleaf_object_init_str(&o_key,kv->key_str);
			}
			else if (g_config.key_type == CL_INT) {
				citrusleaf_object_init_int(&o_key,kv->key_int);
			}
			else {
				if (!g_config.silent) fprintf(stderr, " read thread: unsupport key type %d\n",g_config.key_type);
			}
		
			cf_digest d;
			citrusleaf_calculate_digest(g_config.table, &o_key, &d);
#ifdef PRINT_KEY			
			fprintf(stderr, "get: key %d digest %"PRIx64"\n",key_int, *(uint64_t *)&d);
#endif			

			memcpy( &digests[j], &d, sizeof(cf_digest) );
			
		}
		
		// do the actual work of reading
		uint64_t start_time = cf_getms();
		if (CITRUSLEAF_OK != (rv = citrusleaf_get_many_digest(work->asc, g_config.ns, digests, g_config.n_batch, values, 1, false, batch_getmany_cb, &gm_udata))) {
            uint64_t stop_time = cf_getms();
            uint64_t diff = safe_delta_ms(start_time, stop_time);

        	if (rv == CITRUSLEAF_FAIL_TIMEOUT || rv == CITRUSLEAF_FAIL_SERVERSIDE_TIMEOUT) {
                histogram_insert_data_point(g_p_read_timeout_histogram, diff);

                if (!g_config.silent) {
            		fprintf(stderr, "citrusleaf get many timeout %d %"PRIu64"\n",rv,diff);
                }
        	}
        	else {
                if (!g_config.silent) {
                	fprintf(stderr, "read: failed with error %d\n",rv);
                }
        	}
			citrusleaf_object_free(&values[0].object);
			if (g_config.continuous == false) {
				for (int j = 0; j < g_config.n_batch; j++)
					free(kvs[j]);
				return((void *)-1);
			}
		}
		uint64_t stop_time = cf_getms();
		histogram_insert_data_point(g_p_read_histogram,
			safe_delta_ms(start_time, stop_time));
		
		for (int j = 0; j < g_config.n_batch; j++) {
			free(kvs[j]);
			kvs[j] = 0;
		}
		
		atomic_int_add(work->completed, g_config.n_batch);
		
		if (g_config.delay) {
			if (g_config.delay >= 1000) {
				usleep( (g_config.delay - 1000) * 1000 );
			}
			else {
				if ( (delay_factor++ % (1000 - g_config.delay)) == 0) {
					usleep( 1000 );
				}
			}
		}
		
	}
	return((void *)0);
}

#define MAX_UINT32 0xFFFFFFFF

void *
write_fn(void *udata)
{
	write_work *work = (write_work *) udata;
	uint32_t delay_factor = 0;

	if (g_config.verbose) fprintf(stderr, "write worker thread: n_writes %d start_key %d \n",work->n_writes, work->start_key);
    
	for (uint i=0;i<work->n_writes;i++) {
	
		key_value *kv = make_key_value(work->start_key + i, work->key_len, work->value_len);
	
		// Create a key for accessing
		cl_object o_key;
		if (g_config.key_type == CL_STR) {
			citrusleaf_object_init_str(&o_key,kv->key_str);
		}
		else if (g_config.key_type == CL_INT) {
			citrusleaf_object_init_int(&o_key,kv->key_int);
		}
		else {
			if (!g_config.silent) fprintf(stderr, " read thread: unsupport key type %d\n",g_config.key_type);
		}


#ifdef PRINT_KEY
		cf_digest d;
		citrusleaf_calculate_digest(g_config.table, &o_key, &d);
		fprintf(stderr, "set: key %d digest %"PRIx64"\n",(int)kv->key_int, *(uint64_t *)&d);
#endif

		uint32_t r_gen = MAX_UINT32;
		if (g_config.read_modify_write) {
			// bundle it into a value array
			cl_bin rvalues[1];
			memset(rvalues, 0, sizeof(rvalues));
			strcpy(rvalues[0].bin_name, g_config.bin);
		
			// do the actual work of reading
			int rrv;
			if (CITRUSLEAF_OK != (rrv = citrusleaf_get(work->asc, g_config.ns, g_config.table, &o_key, rvalues, 1, g_config.timeout, &r_gen))) {
				if (rrv != CITRUSLEAF_FAIL_NOTFOUND) {
					if (!g_config.silent) fprintf(stderr, "read-modify-write: failed with error %d\n",rrv);
					if (g_config.continuous == false) 	return((void *)-1);
				}
				r_gen = MAX_UINT32;
			}
			else { // success			
				if (rvalues[0].object.type != g_config.value_type) {
					raise(SIGINT);
					return((void *)-1);
				}
			}
			// if (!g_config.silent) fprintf(stderr, "generation %d\n",r_gen);
			citrusleaf_object_free(&rvalues[0].object);
		}
		// set a value
		cl_bin values[1];
		strcpy(values[0].bin_name, g_config.bin);

		if (g_config.value_type == CL_STR) {
			citrusleaf_object_init_str(&values[0].object, kv->value_str);
		}
		else if (g_config.value_type == CL_BLOB) {
			citrusleaf_object_init_blob(&values[0].object, kv->value_blob, kv->value_blob_len);
// very verbose but sometimes necessary			
//			dump_buf("writing blob: ",kv->value_blob, kv->value_blob_len);
		}
		else if (g_config.value_type == CL_INT) {
			citrusleaf_object_init_int(&values[0].object, kv->value_int);
		}
		else {
			if (!g_config.silent) fprintf(stderr, "set: unknown type %d! misconfig! bail!\n",g_config.value_type);	
		}
		
		cl_write_parameters cl_wp;
		cl_write_parameters_set_default(&cl_wp);
		cl_wp.timeout_ms = g_config.timeout;
		cl_wp.record_ttl = g_config.record_ttl + (g_config.ttl_range != 0 ? rand_64() % g_config.ttl_range : 0);
		if (g_config.read_modify_write) {
			if (r_gen == MAX_UINT32) 
				cl_wp.unique = true; // if it didn't exist before, do set unique
			else
				cl_write_parameters_set_generation(&cl_wp, r_gen);	// did exist, use the correct gen
		}
        
		cl_rv rv;
		uint64_t start_time = cf_getms();
		if (CITRUSLEAF_OK != (rv = citrusleaf_put(work->asc, g_config.ns, g_config.table, &o_key, values, 1, &cl_wp))) {
            uint64_t stop_time = cf_getms();
            uint64_t diff = safe_delta_ms(start_time, stop_time);

        	if (rv == CITRUSLEAF_FAIL_TIMEOUT || rv == CITRUSLEAF_FAIL_SERVERSIDE_TIMEOUT) {
                histogram_insert_data_point(g_p_write_timeout_histogram, diff);

                if (!g_config.silent) {
            		fprintf(stderr, "citrusleaf put timeout %d %"PRIu64" %d\n",rv,diff,cl_wp.timeout_ms);
                }
        	}
        	else {
                if (!g_config.silent) {
            		cf_digest d;
            		citrusleaf_calculate_digest(g_config.table, &o_key, &d);
            		fprintf(stderr, "citrusleaf put failed %d %"PRIx64"\n",rv,*(uint64_t *)&d);
                }
        	}
			
			if ((rv == CITRUSLEAF_FAIL_GENERATION) ||
				(rv == CITRUSLEAF_FAIL_KEY_EXISTS) ) {
				atomic_int_add(work->gen_errs, 1);			
			}
			if (g_config.continuous == false)
				return((void *)-1);
		}
		uint64_t stop_time = cf_getms();

		histogram_insert_data_point(g_p_write_histogram,
		safe_delta_ms(start_time, stop_time));

		free(kv);
		atomic_int_add(work->completed, 1);
		
		if (g_config.delay) {
			if (g_config.delay >= 1000) {
				usleep( (g_config.delay - 1000) * 1000 );
			}
			else {
				if ( (delay_factor++ % (1000 - g_config.delay)) == 0) {
					usleep( 1000 );
				}
			}
		}

		
	}	
	return(0);
}

void *
delete_fn(void *udata)
{	
	delete_work *work = (delete_work *) udata;
	uint32_t delay_factor = 0;

	if (g_config.verbose) fprintf(stderr, "delete worker thread: n_deletes %d start_key %d \n",work->n_deletes, work->start_key);

	for (uint i=0;i<work->n_deletes;i++) {

		key_value *kv = make_key_value(work->start_key + i, work->key_len, work->value_len);

		// Create a key for accessing
		cl_object o_key;
		if (g_config.key_type == CL_STR) {
			citrusleaf_object_init_str(&o_key,kv->key_str);
		}
		else if (g_config.key_type == CL_INT) {
			citrusleaf_object_init_int(&o_key,kv->key_int);
		}
		else {
			if (!g_config.silent) fprintf(stderr, " read thread: unsupport key type %d\n",g_config.key_type);
		}


#ifdef PRINT_KEY
		cf_digest d;
		citrusleaf_calculate_digest(g_config.table, &o_key, &d);
		fprintf(stderr, "set: key %d digest %"PRIx64"\n",(int)kv->key_int, *(uint64_t *)&d);
#endif

		// Set write parameters
		cl_write_parameters cl_wp;
		cl_write_parameters_set_default(&cl_wp);
		cl_wp.timeout_ms = g_config.timeout;
		cl_wp.record_ttl = g_config.record_ttl + (g_config.ttl_range != 0 ? rand_64() % g_config.ttl_range : 0);

		cl_rv rv;
		uint64_t start_time = cf_getms();
		if (CITRUSLEAF_OK != (rv = citrusleaf_delete(work->asc, g_config.ns, g_config.table, &o_key, &cl_wp))) {
			uint64_t stop_time = cf_getms();
			uint64_t diff = safe_delta_ms(start_time, stop_time);

			if (rv == CITRUSLEAF_FAIL_TIMEOUT || rv == CITRUSLEAF_FAIL_SERVERSIDE_TIMEOUT) {
				histogram_insert_data_point(g_p_delete_timeout_histogram, diff);

				if (!g_config.silent) {
					fprintf(stderr, "citrusleaf delete timeout %d %"PRIu64" %d\n",rv,diff,cl_wp.timeout_ms);
				}
			}
			else {
				if (!g_config.silent) {
					cf_digest d;
					citrusleaf_calculate_digest(g_config.table, &o_key, &d);
					fprintf(stderr, "citrusleaf delete failed %d %"PRIx64"\n",rv,*(uint64_t *)&d);
				}
			}

			if ((rv == CITRUSLEAF_FAIL_GENERATION) ||
					(rv == CITRUSLEAF_FAIL_KEY_EXISTS) ) {
				atomic_int_add(work->gen_errs, 1);			
			}
			if (g_config.continuous == false)
				return((void *)-1);
		}
		uint64_t stop_time = cf_getms();

		histogram_insert_data_point(g_p_delete_histogram,
				safe_delta_ms(start_time, stop_time));

		free(kv);
		atomic_int_add(work->completed, 1);

		if (g_config.delay) {
			if (g_config.delay >= 1000) {
				usleep( (g_config.delay - 1000) * 1000 );
			}
			else {
				if ( (delay_factor++ % (1000 - g_config.delay)) == 0) {
					usleep( 1000 );
				}
			}
		}


	}	
	return(0);
}


/*This function does operations (reads and writes) n_ops number of times. 
 * The writes and reads will be in W:(100-W) ratio. It will read and write 
 * random keys, Load keys before using this function to avoid 
 * "keys not found" error.
*/

void *
ops_fn(void *udata)
{
	op_work *work = (op_work *) udata;
	write_work *workw = (write_work * )malloc(sizeof(write_work));
	read_work *workr = (read_work * )malloc(sizeof(read_work));

	int iteration = (int)((work->n_ops)/100);
	int residue = (work->n_ops)%100;
	uint read_start_key =  work->start_key;
	uint write_start_key = work->start_key;
	work->writeratio = my_min(100, work->writeratio);

	//writing snd reading n_ops times in a ratio writeratio : (100-writepesr100)
	for (uint i=0;i<iteration;i++) {
		for(int j=0; j<work->writeratio; j++) {
        		write_start_key = (rand_64()%work->n_keys) + work->start_key;
			//writing
			workw->completed = work->wcompleted;
			workw->gen_errs = work->gen_errs;
			workw->key_len = work->key_len;
			workw->value_len = work->value_len;
			workw->start_key = write_start_key;
			workw->value_len = work->value_len;
			workw->asc = work->asc;
			workw->id = 0;
			workw->n_writes = 1;
			write_fn(workw);
		}

		for(int j=0; j<100-work->writeratio; j++) {	
			//reading
			workr->completed = work->rcompleted;
			workr->key_len = work->key_len;
			workr->value_len = work->value_len;
			workr->start_key = read_start_key;
			workr->value_len = work->value_len;
			workr->asc = work->asc;
			workr->n_keys = work->n_keys;
			workr->id = 0;
			workr->n_reads = 1;
			read_fn(workr);
		}
	}
	//Residue
	int work_residue = (int)(residue*work->writeratio/100);

	for(int j=0; j<work_residue; j++) {
		write_start_key = (rand_64()%work->n_keys) + work->start_key;
		//working
		workw->completed = work->wcompleted;
		workw->gen_errs = work->gen_errs;
		workw->key_len = work->key_len;
		workw->value_len = work->value_len;
		workw->start_key = write_start_key;
		workw->value_len = work->value_len;
		workw->asc = work->asc;
		workw->id = 0;
		workw->n_writes = 1;
		write_fn(workw);
	}
	
	for(int j=0; j<(residue-work_residue); j++) {
		//reading
		workr->completed = work->rcompleted;
		workr->key_len = work->key_len;
		workr->value_len = work->value_len;
		workr->start_key = work->start_key;
		workr->value_len = work->value_len;
		workr->asc = work->asc;
		workr->n_keys = work->n_keys;
		workr->id = 0;
		workr->n_reads = 1;
		read_fn(workr);
	}
	free(workw);
	free(workr);
	return (0);
}

int 
do_key_test ( uint n_keys, uint n_reads, uint n_writes, uint n_deletes, uint n_threads, uint key_len, uint value_len, uint n_ops, uint writeratio, bool parallel)
{

	if((int)n_threads < 1)
        {
                fprintf(stderr, "Error: Not enough threads\n");
                exit(1);
     	 }

	pthread_t	*thr_array = malloc(sizeof(pthread_t) * n_threads);
	g_p_read_histogram = histogram_create();
	g_p_write_histogram = histogram_create();
	g_p_delete_histogram = histogram_create();
    g_p_read_timeout_histogram = histogram_create();
    g_p_write_timeout_histogram = histogram_create();
    g_p_delete_timeout_histogram = histogram_create();
    if (g_p_read_histogram == NULL || g_p_write_histogram == NULL || g_p_delete_histogram == NULL ||
    	g_p_read_timeout_histogram == NULL || g_p_write_timeout_histogram == NULL ||  g_p_delete_timeout_histogram == NULL) {
        fprintf(stderr," cannot create histograms \n");
        return -1;
    }
	// Create a cluster object that all requests will happen on
	cl_cluster *asc = citrusleaf_cluster_create();
	if (g_config.host_direct == true)
		citrusleaf_cluster_follow(asc, false);
	citrusleaf_cluster_add_host(asc, g_config.host, g_config.port, 0);
	fprintf(stderr," client settled, starting test\n");
	
	if (g_config.verbose) fprintf(stderr, "enter do key test: threads: %d value len: %d\n",n_threads,value_len);

	//read and write operation in some x:y ratio..
	if(n_ops == 0) {
		if (n_writes) {

			if (g_config.verbose) fprintf(stderr,"starting write loop: writing %d elements with %d threads\n",n_writes, n_threads);
			write_work  *write_array = malloc(sizeof(write_work)*n_threads);
			uint writes_per_thread = n_writes / n_threads;
			uint total_writes = 0;
			atomic_int *completed = atomic_int_create(0);
			atomic_int *gen_errs = atomic_int_create(0);

			void *c_th = start_counter_thread( "writes: ",completed,gen_errs);		

			for (uint i=0;i<n_threads;i++) {			
				write_work *work = &write_array[i];

				work->completed = completed;
				work->gen_errs = gen_errs;
				work->key_len = key_len;
				work->value_len = value_len;			
				
				work->value_len = value_len;
				work->asc = asc;
				work->id = i;

				// set all keys in parallel on all threads
				if (parallel) {
					work->start_key = g_config.zero;
					work->n_writes = writes_per_thread;
				}
				else {
					work->start_key = total_writes + g_config.zero;
					if (i == n_threads-1) {// last time! use all remaining writes
						work->n_writes = n_writes - total_writes;
					}
					else {                // regular loop	
						work->n_writes = writes_per_thread;
					}
					total_writes += work->n_writes;
				}

				pthread_create(&thr_array[i], 0, write_fn, work); 
			}

			// wait for threads to complete
			for (uint i=0;i<n_threads;i++) {
				void *value_ptr;
				pthread_join(thr_array[i], &value_ptr);
			}
			stop_counter_thread(c_th);
			free(write_array);
			atomic_int_destroy(completed);
		}

                if (n_reads) {
                        if (g_config.verbose) fprintf(stderr,"starting read loop: reading %d elements with %d threads\n",n_reads,n_threads);
                        read_work  *read_array = malloc(sizeof(read_work)*n_threads);
                        uint reads_per_thread = n_reads / n_threads;
                        uint total_reads = 0;
                        atomic_int *completed = atomic_int_create(0);

                        void *c_th = start_counter_thread( "reads: ", completed, 0);
                        for (uint i=0;i<n_threads;i++) {
                                read_work *work = &read_array[i];
                                work->completed = completed;
                                work->start_key = g_config.zero;
                                work->n_keys = n_keys;
                                work->key_len = key_len;
                                work->value_len = value_len;
                                work->asc = asc;
                                work->id = i;

                                if (i == n_threads-1) // last time! use all remaining writes
                                        work->n_reads = n_reads - total_reads;
                                else                 // regular loop    
                                        work->n_reads = reads_per_thread;
                                total_reads += work->n_reads;
                                pthread_create(&thr_array[i], 0, g_config.n_batch == 0 ? read_fn : read_batch_fn , work);
                        }
                        // wait for threads to complete
                        for (uint i=0;i<n_threads;i++) {
                                void *value_ptr;
                                pthread_join(thr_array[i], &value_ptr);
                        }
                        stop_counter_thread(c_th);
                        free(read_array);
                        atomic_int_destroy(completed);
                }

		if (n_deletes) {

			if (g_config.verbose) fprintf(stderr,"starting write loop: writing %d elements with %d threads\n",n_deletes, n_threads);
			delete_work  *delete_array = malloc(sizeof(delete_work)*n_threads);
			uint deletes_per_thread = n_deletes / n_threads;
			uint total_deletes = 0;
			atomic_int *completed = atomic_int_create(0);
			atomic_int *gen_errs = atomic_int_create(0);

			void *c_th = start_counter_thread( "deletes: ",completed,gen_errs);

			for (uint i=0;i<n_threads;i++) {
				delete_work *work = &delete_array[i];

				work->completed = completed;
				work->gen_errs = gen_errs;
				work->key_len = key_len;
				work->value_len = value_len;

				work->value_len = value_len;
				work->asc = asc;
				work->id = i;
				// set all keys in parallel on all threads
				if (parallel) {
					work->start_key = g_config.zero;
					work->n_deletes = deletes_per_thread;
				}
				else {
					work->start_key = total_deletes + g_config.zero;
					if (i == n_threads-1) {// last time! use all remaining writes
						work->n_deletes = n_deletes - total_deletes;
					}
					else {                // regular loop   
						work->n_deletes = deletes_per_thread;
					}
					total_deletes += work->n_deletes;
				}

				pthread_create(&thr_array[i], 0, delete_fn, work);
			}

			// wait for threads to complete
			for (uint i=0;i<n_threads;i++) {
				void *value_ptr;
				pthread_join(thr_array[i], &value_ptr);
			}
			stop_counter_thread(c_th);
			free(delete_array);
			atomic_int_destroy(completed);
		}
	}
	else {
		if((int)n_ops < 1) {
			fprintf(stderr, "Error: Invalid number of operation.\n");
			exit(1);
		}
		if (g_config.verbose) fprintf(stderr,"starting writing and reading in ratio %d : %d\n",writeratio, 100-writeratio);
			
		op_work  *op_array = malloc(sizeof(op_work)*n_threads);
		uint ops_per_thread = n_ops / n_threads;
		uint total_ops = 0;
		atomic_int *rcompleted = atomic_int_create(0);
		atomic_int *wcompleted = atomic_int_create(0);
		atomic_int *gen_errs = atomic_int_create(0);

		void *c_rth = start_counter_thread( "reads: ",rcompleted,gen_errs);		
		void *c_wth = start_counter_thread( "writes: ",wcompleted,gen_errs);		

		writeratio = my_min(100, writeratio);
		writeratio = my_max(0, (int)writeratio);
		for (uint i=0;i<n_threads;i++) {			
			op_work *work = &op_array[i];
			work->rcompleted = rcompleted;
			work->wcompleted = wcompleted;
			work->gen_errs = gen_errs;
			work->key_len = key_len;
			work->value_len = value_len;			
			work->start_key = g_config.zero;
			work->value_len = value_len;
			work->asc = asc;
			work->id = i;
			work->writeratio = writeratio;
			work->n_keys = n_keys;

			if (i == n_threads-1) {// last time! use all remaining writes
				work->n_ops = n_ops - total_ops;
			}
			else {                // regular loop	
				work->n_ops = ops_per_thread;
			}

			total_ops += work->n_ops;

			pthread_create(&thr_array[i], 0, ops_fn, work); 
		}
		
		// wait for threads to complete
		for (uint i=0;i<n_threads;i++) {
			void *value_ptr;
			pthread_join(thr_array[i], &value_ptr);
		}
		stop_counter_thread(c_rth);
		stop_counter_thread(c_wth);
		free(op_array);
		atomic_int_destroy(wcompleted);
		atomic_int_destroy(rcompleted);
	}
	
    histogram_dump(g_p_write_histogram,  "writes");
    histogram_dump(g_p_write_timeout_histogram,  "write timeouts");
    histogram_dump(g_p_read_histogram,  "reads");
    histogram_dump(g_p_read_timeout_histogram,  "read timeouts");
    histogram_dump(g_p_delete_histogram,  "deletes");
    histogram_dump(g_p_delete_timeout_histogram,  "delete timeouts");

	free(thr_array);
	citrusleaf_cluster_destroy(asc);

	return(-1);	
}
