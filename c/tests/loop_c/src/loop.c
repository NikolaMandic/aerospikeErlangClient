/*
 *  Citrusleaf test
 *  loop.c - A key-value oriented, looping test that allows stress testing
 * of inserts, deletes, etc
 *
 *   Brian Bulkowski
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

#include "citrusleaf/citrusleaf.h"
#include "loop.h"


// #define DEBUG 1
// #define DEBUG_VERBOSE 1

typedef struct key_value_s {
	char *key;
	char *value;
} key_value;

//
// Convert a uint to a number sz wide using front-zero padding
// to fill the space
//


void
my_itoa(char *s, uint64_t v, uint sz)
{
	s[sz] = 0;
	int i;
	for (i = sz-1; i >= 0; i--) {
		s[i] = (v % 10) + '0';
		if (v) v = v / 10;
	}
	
}


int
write_new_value(uint32_t key, cl_object *key_o, cf_digest *d)
{
	uint64_t new_value_int;
	do {
		new_value_int = rand_64();
	} while (new_value_int == VALUE_UNINIT || new_value_int == VALUE_DELETED);
	
	g_config.values[key] = new_value_int;
	char new_value_str[g_config.value_len+1];
	my_itoa(new_value_str, new_value_int, g_config.value_len); 
	
	cl_bin values[1];
	strcpy(values[0].bin_name, g_config.bin);
	citrusleaf_object_init_str(&values[0].object, new_value_str);

	cl_write_parameters cl_w_p;	
	cl_write_parameters_set_default(&cl_w_p);
	cl_w_p.timeout_ms = g_config.timeout_ms;
	
	int rv;
	rv = citrusleaf_put(g_config.asc, g_config.ns, g_config.set, key_o, values, 1, &cl_w_p);
	if (rv != 0) {
		fprintf(stderr, "aerospike put returned error %d, fail digest %"PRIx64"\n",rv, *(uint64_t *)d);
		if (g_config.strict)
			return(-1);
	}
	
	citrusleaf_object_init(&values[0].object);
	
	rv = citrusleaf_verify(g_config.asc, g_config.ns, g_config.set, key_o, values, 1, g_config.timeout_ms, NULL);
	if (rv != 0) {
		fprintf(stderr, "aerospike get returned error %d digest %"PRIx64"\n",rv, *(uint64_t *)d);
		if (g_config.strict)
			return(-1);
	}

	// test!
	if (values[0].object.type != CL_STR) {
		fprintf(stderr, "read value has wrong type: expect string (3) got %d, fail digest %"PRIx64"\n",(int)values[0].object.type, *(uint64_t *)d);
		if (g_config.strict)
			return(-1);
	}
	if (strcmp(values[0].object.u.str, new_value_str) != 0) {
		fprintf(stderr, "read value does not match set value. digest %"PRIx64"\n", *(uint64_t *)d);
		fprintf(stderr, "  expecting: %s\n",new_value_str);
		fprintf(stderr, "  got: %s\n",values[0].object.u.str);
		if (g_config.strict)
			return( -1);
	}
	
	citrusleaf_object_free(&values[0].object);
	
	atomic_int_add(g_config.read_counter, 1);
	atomic_int_add(g_config.write_counter, 1);

	
	return(0);
}


void *
work_fn(void *gcc_is_ass)
{
	// Forever,
	do {
		// Pick a key to use. Look it up to see if anyone else is using it.
		uint32_t	key = rand_64() % g_config.n_keys;
		
		uint32_t    die = rand_64() & 0x03;
		
		if (SHASH_OK == shash_put_unique(g_config.in_progress_hash, &key, 0) ) 
		{
			cl_rv rv;
		
			// Make the key into a string
			char key_s[g_config.key_len+1];
			my_itoa(key_s, key, g_config.key_len);
			
			// Make an cl_object that represents the key
			cl_object key_o;
			citrusleaf_object_init_str(&key_o, key_s);
			

			cf_digest d;
			citrusleaf_calculate_digest(g_config.set, &key_o, &d);

			if (VALUE_UNINIT == g_config.values[key]) {

				// simply set the value to something - can't really check anything because we don't know the state				
				if (0 != write_new_value(key, &key_o, &d)) {
					if (g_config.strict)   					goto Fail;
				}
				atomic_int_add(g_config.key_counter, 1);
			}
			else if (VALUE_DELETED == g_config.values[key]) {
				
				// Shouldn't exist
				cl_bin *cl_v = 0;
				int		cl_v_len;
				rv = citrusleaf_get_all(g_config.asc, g_config.ns, g_config.set, &key_o, &cl_v, &cl_v_len, g_config.timeout_ms, NULL);
				if (rv != CITRUSLEAF_FAIL_NOTFOUND) {
					fprintf(stderr, "Get after delete returned improper value when should be deleted %d key %s digest %"PRIx64"\n",rv,key_s, *(uint64_t *)&d);
					if (g_config.strict)    goto Fail;
				}
				if (cl_v)	free(cl_v);

				atomic_int_add(g_config.read_counter, 1);  // did two ops here						
				
				// write a new value
				if (die < 2) {
					if (0 != write_new_value(key, &key_o, &d)) {
						if (g_config.strict)   goto Fail;
					}
					atomic_int_add(g_config.key_counter, 1);
				}
			}
			// Value is well known. Check to see that it's still right.
			else {			
				
				cl_bin values[1];
				strcpy(values[0].bin_name, g_config.bin);
				citrusleaf_object_init(&values[0].object);
				
				// Make string version of old value for checking
				char new_value_str[g_config.value_len+1];
				my_itoa(new_value_str, g_config.values[key], g_config.value_len); 
				citrusleaf_object_init_str(&values[0].object, new_value_str);
				
				rv = citrusleaf_verify(g_config.asc, g_config.ns, g_config.set, &key_o, values, 1, g_config.timeout_ms, NULL);
				if (rv != 0) {
					fprintf(stderr, "Get returned improper value %d when should be set : key %d digest %"PRIx64"\n",rv,key, *(uint64_t *)&d);
					if (g_config.strict)   goto Fail;
					goto V1;
				}
				
				// test!
				if (values[0].object.type != CL_STR) {
					fprintf(stderr, "read value has wrong type: expect string (3) got %d\n",(int)values[0].object.type);  
					if (g_config.strict)   return((void *)-1);
				}
				else if (strcmp(values[0].object.u.str, new_value_str) != 0) {
					fprintf(stderr, "read value does not match set value.\n");
					fprintf(stderr, "  expecting: %s\n",new_value_str);
					fprintf(stderr, "  got: %s\n",values[0].object.u.str);
					if (g_config.strict)   goto Fail;
				}
				
				citrusleaf_object_free(&values[0].object);
				atomic_int_add(g_config.read_counter, 1);
				
				// Delete, write new value, what's your pleasure?
			V1:				
				if (die < 2) {
					if (0 != write_new_value(key, &key_o, &d)) {
						if (g_config.strict)   return((void *)-1);
					}
				}
				// Delete!
				else if (die == 2) {
					rv = citrusleaf_delete_verify(g_config.asc, g_config.ns, g_config.set, &key_o, 0);
					if (rv != 0) {
						fprintf(stderr, "Delete returned improper value %d, fail: key %d digest %"PRIx64"\n",rv, key, *(uint64_t *)&d);
						if (g_config.strict)   goto Fail;
					}

					cl_bin values[1];
					strcpy(values[0].bin_name, g_config.bin);
					citrusleaf_object_init(&values[0].object);
					
					rv = citrusleaf_get(g_config.asc, g_config.ns, g_config.set, &key_o, values, 1, g_config.timeout_ms, NULL);
					if (rv != CITRUSLEAF_FAIL_NOTFOUND) {
						fprintf(stderr, "Get after delete returned improper value %d digest %"PRIx64"\n",rv, *(uint64_t *)&d);
						if (g_config.strict)   goto Fail;
					}
					
					citrusleaf_object_free(&values[0].object);
					
					g_config.values[key] = VALUE_DELETED;
					atomic_int_add(g_config.read_counter, 1);  // did two ops here
					atomic_int_add(g_config.delete_counter, 1);
					atomic_int_add(g_config.key_counter, -1);
					
				}
				
			}
			
			// remove my lock on this key
			shash_delete(g_config.in_progress_hash, &key);
			

		}		
	} while (1);
	
	
Fail:	
	abort();
	return((void *)-1);
}



uint32_t progress_hash_fn(void *key)
{
	uint32_t	value = *(uint32_t *)key;
	return(value);
}



int 
do_loop_test ( )
{
	fprintf(stderr, "starting test\n");
	
	// this hash is the rendez-vous for all the worker threads, to make sure they're not working
	// on the same keys at the same time. It starts out empty. When a worker wants to use a key, it
	// drops an element in the hash unique.
	shash_create(&g_config.in_progress_hash, progress_hash_fn, sizeof(uint32_t), 0, g_config.n_threads * 2, SHASH_CR_MT_BIGLOCK);

	// Create the aerospike cluster
	g_config.asc = citrusleaf_cluster_create();
	citrusleaf_cluster_add_host(g_config.asc, g_config.host, g_config.port, 0);

	if (g_config.follow == false) 
		citrusleaf_cluster_follow(g_config.asc, false);
	
	
	// This array is the current value of a given key. Starts out as uninitialized.
	g_config.values = malloc( sizeof(uint64_t) * g_config.n_keys);
	if (g_config.values == 0) {
		fprintf(stderr, " could not malloc %d, use fewer keys",(int)(sizeof(uint64_t)*g_config.n_keys));
		return(-1);
	}
	for (uint i=0;i<g_config.n_keys;i++)
		g_config.values[i] = VALUE_UNINIT;

	g_config.read_counter = atomic_int_create(0);
	g_config.write_counter = atomic_int_create(0);
	g_config.delete_counter = atomic_int_create(0);
	g_config.key_counter = atomic_int_create(0);
	
	void *counter_control = start_counter_thread( g_config.read_counter, g_config.write_counter, g_config.delete_counter, g_config.key_counter);
	
	// Create the threads and send them on their way
	pthread_t	*thr_array = malloc(sizeof(pthread_t) * g_config.n_threads);

	fprintf(stderr, "starting threads for test\n");
	for (uint i=0;i<g_config.n_threads;i++)
		pthread_create(&thr_array[i], 0, work_fn, 0); 
	
	for (uint i=0;i<g_config.n_threads;i++) {
		void *value_ptr;
		pthread_join(thr_array[i], &value_ptr);
		if ( ((int)(ssize_t)value_ptr) )
			fprintf(stderr, "thread %d returned value %d\n",i,(int) (ssize_t) value_ptr);
	}
	
	free(g_config.values);
	free(thr_array);
	citrusleaf_cluster_destroy(g_config.asc);
	
	stop_counter_thread(counter_control);
	
	return(-1);	
}
