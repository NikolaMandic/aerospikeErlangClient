/*
 *  Citrusleaf Tools
 *  src/ascli.c - command-line interface
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
#include <stdbool.h>
#include <getopt.h>
#include <fcntl.h>  // open the rnadom file


#include "citrusleaf/citrusleaf.h"
#include "citrusleaf/cl_shm.h"
#include "key.h"

// get the nice juicy SSL random bytes
#include <openssl/rand.h>


// #define DEBUG 1

atomic_int	*
atomic_int_create(uint32_t val)
{
	atomic_int *ai = malloc(sizeof(atomic_int));
	ai->val = val;
	pthread_mutex_init(&ai->lock,0);
	return(ai);
}

void			
atomic_int_destroy(atomic_int *ai)
{
	pthread_mutex_destroy(&ai->lock);
	free(ai);
}


uint32_t
atomic_int_add(atomic_int *ai, uint32_t val)
{
	uint32_t	rv;
	pthread_mutex_lock(&ai->lock);
	ai->val += val;
	rv = ai->val;
	pthread_mutex_unlock(&ai->lock);
	return(rv);
}

uint32_t		
atomic_int_get(atomic_int *ai)
{
	uint32_t	val;
	pthread_mutex_lock(&ai->lock);
	val = ai->val;
	pthread_mutex_unlock(&ai->lock);
	return(val);
}

typedef struct {
	char *msg;
	atomic_int		*keys;
	atomic_int		*gen_errs;

	int				death;
	pthread_t		th;

} counter_thread_control;

void *
counter_fn(void *arg)
{
	counter_thread_control *ctc = (counter_thread_control *) arg;
	
	uint64_t last_t = 0;
	
	while (ctc->death == 0) {
		
		sleep(1);
		
		int keys = atomic_int_get(ctc->keys);
		int tps = keys - last_t;
		last_t = keys;
		
		if ((ctc->gen_errs == 0) || (atomic_int_get(ctc->gen_errs) == 0) ) 
			fprintf(stderr, "%s: %d (tps %d)\n",ctc->msg,keys,tps);
		else
			fprintf(stderr, "%s: %d (tps %d) gen-errs %d\n",ctc->msg,keys,tps,atomic_int_get(ctc->gen_errs));
	}
	return(0);
}

void *
start_counter_thread(char *msg, atomic_int *keys, atomic_int *gen_errs)
{
	counter_thread_control *ctc = (counter_thread_control *) malloc(sizeof(counter_thread_control));
	ctc->keys = keys;
	ctc->gen_errs = gen_errs;
	ctc->msg = strdup(msg);

	ctc->death = 0;
	pthread_create(&ctc->th, 0, counter_fn, ctc);
	return(ctc);
}


void
stop_counter_thread(void *control)
{
	counter_thread_control *ctc = (counter_thread_control *)control;
	ctc->death = 1;
	pthread_join(ctc->th, 0);
	free(ctc);
}

//
// Buffer up the random numbers.
//

#define SEED_SZ 64
static uint8_t rand_buf[1024 * 8];
static uint rand_buf_off = 0;
static int	seeded = 0;
static pthread_mutex_t rand_buf_lock = PTHREAD_MUTEX_INITIALIZER;

uint64_t
rand_64()
{
	pthread_mutex_lock(&rand_buf_lock);
	if (rand_buf_off < sizeof(uint64_t) ) {
		if (seeded == 0) {
			int rfd = open("/dev/urandom",	O_RDONLY);
			int rsz = read(rfd, rand_buf, SEED_SZ);
			if (rsz < SEED_SZ) {
				fprintf(stderr, "warning! can't seed random number generator");
				return(0);
			}
			close(rfd);
			RAND_seed(rand_buf, rsz);
			seeded = 1;
		}
		if (1 != RAND_bytes(rand_buf, sizeof(rand_buf))) {
			fprintf(stderr, "RAND_bytes not so happy.\n");
			pthread_mutex_unlock(&rand_buf_lock);
			return(0);
		}
		rand_buf_off = sizeof(rand_buf);
	}
	
	rand_buf_off -= sizeof(uint64_t);
	uint64_t r = *(uint64_t *) (&rand_buf[rand_buf_off]);
	pthread_mutex_unlock(&rand_buf_lock);
	return(r);
}

#define TEST_STRING "value"

//
// One shot test - good for basic functionality
// An example of reading and writing one key
// 
int 
test_one_key(void)
{
	cl_cluster *asc;
	asc = citrusleaf_cluster_create();
	citrusleaf_cluster_follow(asc, false); // just this host!
	citrusleaf_cluster_add_host(asc, g_config.host, g_config.port,0);

	// Create a key for accessing
	cl_object o_key;
	citrusleaf_object_init_str(&o_key,"key");
	
	// set a value
	cl_bin values[1];
	strcpy(values[0].bin_name, g_config.bin);
	citrusleaf_object_init_str(&values[0].object, TEST_STRING);
	
	citrusleaf_put(asc, g_config.ns, g_config.table, &o_key, values, 1, 0);

	// now let's pull it out again
	memset(values, 0, sizeof(values));
	strcpy(values[0].bin_name, g_config.bin);

	citrusleaf_get(asc, g_config.ns, g_config.table, &o_key, values, 1, 0, NULL);
	
	if (values[0].object.type != CL_STR) {
		fprintf(stderr, "Test failed, type in correct, expected %d received %d\n",
			(int)CL_STR, (int)values[0].object.type);
		return(-1);
	}
	
	if (values[0].object.sz != strlen(TEST_STRING)) {
		fprintf(stderr, "Test failed, length incorrect, got %d expected %d\n",
			(int) values[0].object.sz, (int) strlen(TEST_STRING));
		return(-1);
	}
		
	if (strcmp(values[0].object.u.str, TEST_STRING) != 0) {
		fprintf(stderr, "Test failed, string incorrect, got %s expected %s\n",
			values[0].object.u.str, TEST_STRING);
		return(-1);
	}
		
	// have to hand back any possible malloced memory
	citrusleaf_object_free(&values[0].object);

	citrusleaf_cluster_destroy(asc);
	
	fprintf(stderr, "TEST SUCCESS\n");
	return(0);
}

/* SYNOPSIS */
/* this is a simple test to excersize the sort system.
   Especially good for telling how optimal the code is.
*/

// This is random 64 bit numbers with holes.
// might not fit your pattern of use....

uint64_t *
random_binary_array( uint nelems )
{
	uint64_t *a = malloc( nelems * sizeof(uint64_t) );
	
	RAND_bytes((void *) a, nelems * sizeof(uint64_t ) );
	
	return(a);
	
}


void usage(void) {
	fprintf(stderr, "Usage key_c:\n");
	fprintf(stderr, "-h host [default 127.0.0.1] \n");
	fprintf(stderr, "-p port [default 3000]\n");
	fprintf(stderr, "-n namespace [default test]\n");
	fprintf(stderr, "-S set [default none (empty string)]\n");
	fprintf(stderr, "-t threads [default 32]\n");
	fprintf(stderr, "-k keys [default 10000]\n");
	fprintf(stderr, "-c check: sends all data with a get request for server-side validation\n");
	fprintf(stderr, "-r reads [default 10000]\n");
	fprintf(stderr, "-w writes [default 10000]\n");
	fprintf(stderr, "-e deletes [default 0]\n");
	fprintf(stderr, "-M use read-modify-write, i.e., read generation and set it while writing\n");
	fprintf(stderr, "-V is size of the value [default 100]\n");
	fprintf(stderr, "-m timeout in millisec [default ?]\n");
	fprintf(stderr, "-1 just run one-test\n");
	fprintf(stderr, "-d is direct to this one host\n");
	fprintf(stderr, "-D is the delay factor [default 0]\n");
	fprintf(stderr, "-T is the TTL in seconds to send to the server [default 0]\n");
	fprintf(stderr, "-U is the TTL range in seconds, i.e. from TTL to TTL + range [default 0]\n");
	fprintf(stderr, "-v is verbose\n");
	fprintf(stderr, "-s is silent. Silent shuts up very important errors, so use sparingly\n");
	fprintf(stderr, "-i is integer mode [default: string]\n");
	fprintf(stderr, "-b is blob mode [default: string]\n");
	fprintf(stderr, "-z is the zero point: the key to start with [0]\n");
	fprintf(stderr, "-f is value factor: key * f = value [1]\n");
	fprintf(stderr, "-C is continue mode: ignore errors\n");
	fprintf(stderr, "-A is batch reads: [default size 10]\n");
	fprintf(stderr, "-R is to use shared memory [maximum number of nodes supported is 32]\n");
	fprintf(stderr, "-N bin [default is string 'value']\n");
	fprintf(stderr, "-P parallel set all keys on all threads in parallel [default is false]\n");
	fprintf(stderr, "-o number of generic ops with W:(100-W) write:read ratio\n\t[default 0: If non-zero, -w and -r will be ignored. Keep it zero for normal operations]\n");
	fprintf(stderr, "-W number of writes per 100 ops\n\t[default 20: i.e 20 writes per 100 ops. This is applicable to the -o parameter ]\n");
}


config g_config;


int
main(int argc, char **argv)
{
	memset(&g_config, 0, sizeof(g_config));
	
	g_config.host = "127.0.0.1";
	g_config.port = 3000;
	g_config.ns = "test";
	g_config.table = "";
	g_config.bin = "value";
	g_config.verbose = false;
	g_config.host_direct = false;
	g_config.continuous = false;
	g_config.parallel = false;
	g_config.server_validate = false;
	g_config.timeout = 600;
	g_config.key_type = CL_STR;
	g_config.value_type = CL_STR;
	g_config.delay = 0;
	g_config.record_ttl = 0;
	g_config.ttl_range = 0;
	g_config.zero = 0;
	g_config.value_factor = 1;
	g_config.read_modify_write = false;
	
	// parameters for key-test
	int		n_reads = 10000;
	int		n_writes = 10000;
	int		n_deletes = 0;
	int		n_keys = 10000;
	int 		n_threads = 32;
	int		key_len = 100;
	int 		value_len = 128;
	int 		writesper100 = 20;
	int		n_ops = 0;
	// or maybe to simply do one-test
	int		one_test = 0;

	int		c;
	
	printf("testing the C citrusleaf library\n");
	
	while ((c = getopt(argc, argv, "h:p:n:S:t:k:r:w:e:K:V:m:D:T:U:f:z:A:N:W:o:1vdiPIbBCcsMR")) != -1)
	{
		switch (c)
		{
		case 'h':
			g_config.host = strdup(optarg);
			break;
		
		case 'p':
			g_config.port = atoi(optarg);
			break;
		
		case 'n':
			g_config.ns = strdup(optarg);
			break;

		case 'S':
			g_config.table = strdup(optarg);
			break;

		case 't':
			n_threads = atoi(optarg);
			break;
			
		case 'k':
			n_keys = atoi(optarg);
			break;
		
		case 'r':
			n_reads = atoi(optarg);
			break;
		
		case 'w':
			n_writes = atoi(optarg);
			break;

		case 'e':
			n_deletes = atoi(optarg);
			break;
			
		case 'K':
			key_len = atoi(optarg);
			break;

        case 'V':
            value_len = atoi(optarg);
            break;

		case '1':
			one_test = 1;
			break;
			
		case 'v':
			g_config.verbose = true;
			break;

		case 's':
			g_config.silent = true;
			break;
			
		case 'd':
			g_config.host_direct = true;
			break;

		case 'm':
			g_config.timeout = atoi(optarg);
			break;

		case 'f':
			g_config.value_factor = atoi(optarg);
			break;
			
		case 'D':
			g_config.delay = atoi(optarg);
			break;

		case 'T':
			g_config.record_ttl = atoi(optarg);
			break;

		case 'U':
			g_config.ttl_range = atoi(optarg);
			break;

		case 'z':
			g_config.zero = atoi(optarg);
			break;

		case 'i':
			g_config.value_type = CL_INT;
			break;

		case 'I':
			g_config.key_type = CL_INT;
			break;

		case 'b':
			g_config.value_type = CL_BLOB;
			break;
		
		case 'B':
			g_config.key_type = CL_BLOB;
			break;
			
		case 'C':
			g_config.continuous = true;
			break;

		case 'P':
			g_config.parallel = true;
			break;
			
		case 'c':
			g_config.server_validate = true;
			break;
			
		case 'M':
			g_config.read_modify_write = true;
			break;
			
		case 'A':
			g_config.n_batch = atoi(optarg);
			break;

		case 'R':
			g_config.use_shm = true;
			break;

		case 'N':
			g_config.bin = strdup(optarg);
			break;
		case 'W':
			writesper100 = atof(optarg);
			break;

		case 'o':
			n_ops = atof(optarg);
			break;
	
		default:
			usage();
			return(-1);
			
		}
	}
	fprintf(stderr, "testing: host %s port %d namespace %s set %s\n",g_config.host,g_config.port,g_config.ns, g_config.table);
	
	if (g_config.use_shm) {
		//first parameter is maximum number of nodes allowed for this cluster 
		//(shared memory size is accordingly allocated)
		//second parameter is shm_key
		citrusleaf_use_shm(32,788722985);
	}

	citrusleaf_init();

	if (one_test) {
		test_one_key();
	}
	else {
		fprintf(stderr, "key_test: keys: %d reads: %d writes: %d threads: %d\n",n_keys,n_reads,n_writes,n_threads);
		do_key_test(n_keys, n_reads, n_writes, n_deletes, n_threads, key_len, value_len, n_ops, writesper100, g_config.parallel);
	}

	citrusleaf_shutdown();
	return(0);
}
