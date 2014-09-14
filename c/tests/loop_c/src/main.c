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

#include "loop.h"

// get the nice juicy SSL random bytes
#include <openssl/rand.h>


// #define DEBUG 1

atomic_int	*
atomic_int_create(uint64_t val)
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


uint64_t
atomic_int_add(atomic_int *ai, int val)
{
	uint32_t	rv;
	pthread_mutex_lock(&ai->lock);
	ai->val += val;
	rv = ai->val;
	pthread_mutex_unlock(&ai->lock);
	return(rv);
}

uint64_t		
atomic_int_get(atomic_int *ai)
{
	uint64_t	val;
	pthread_mutex_lock(&ai->lock);
	val = ai->val;
	pthread_mutex_unlock(&ai->lock);
	return(val);
}

typedef struct {
	atomic_int		*reads;
	atomic_int		*writes;
	atomic_int		*deletes;
	atomic_int		*keys;
	
	int				death;
	pthread_t		th;
} counter_thread_control;

void *
counter_fn(void *arg)
{
	counter_thread_control *ctc = (counter_thread_control *) arg;
	
	while (ctc->death == 0) {
		sleep(1);
		fprintf(stderr, "loopTest: reads %"PRIu64" writes %"PRIu64" deletes %"PRIu64" (total keys: %"PRIu64")\n",
			atomic_int_get(ctc->reads), atomic_int_get(ctc->writes), atomic_int_get(ctc->deletes),
			atomic_int_get(ctc->keys) );
        	citrusleaf_print_stats();
	}
	return(0);
}

void *
start_counter_thread(atomic_int *reads, atomic_int *writes, atomic_int *deletes, atomic_int *keys)
{
	counter_thread_control *ctc = (counter_thread_control *) malloc(sizeof(counter_thread_control));
	ctc->reads = reads;
	ctc->writes = writes;
	ctc->deletes = deletes;
	ctc->keys = keys;
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
	uint64_t r;
	if (g_config.pseudo_seed) {

		r = random();
		
	}
	else {
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
		r = *(uint64_t *) (&rand_buf[rand_buf_off]);
		pthread_mutex_unlock(&rand_buf_lock);
	}
	return(r);
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
	fprintf(stderr, "-H host:port [default 127.0.0.1:3000]\n");
	fprintf(stderr, "-n namespace [default test]\n");
	fprintf(stderr, "-s set [default 'set']\n");
	fprintf(stderr, "-S Strict - verify all keys and halt on error\n");
	fprintf(stderr, "-b bin [default value]\n");
	fprintf(stderr, "-t threads [default 32]\n");
	fprintf(stderr, "-k keys [default 100000]\n");
	fprintf(stderr, "-K key size [default 10]\n");
	fprintf(stderr, "-V value size [default 20]\n");
	fprintf(stderr, "-r random seed [default random]\n");
	fprintf(stderr, "-m milliseconds timeout [default 200]\n");
	fprintf(stderr, "-f do not follow cluster [default do follow]\n");
	fprintf(stderr, "-v is verbose\n");
}


config g_config;


int
main(int argc, char **argv)
{
	memset(&g_config, 0, sizeof(g_config));
	
	g_config.host = "127.0.0.1";
	g_config.port = 3000;
	g_config.ns = "test";
	g_config.set = "set";
	g_config.bin = "value";
	g_config.verbose = false;
	g_config.follow = true;
	g_config.strict = false;
	
	g_config.n_threads = 32;
	g_config.n_keys = 100000;
	g_config.key_len = 10;
	g_config.value_len = 20;
	g_config.pseudo_seed = 0; // means use a real random value
	g_config.timeout_ms = 200; // 200 ms
	
	g_config.values = 0;
	g_config.in_progress_hash = 0;
	

	int		c;
	
	printf("testing the C citrusleaf library\n");
	
	while ((c = getopt(argc, argv, "h:p:H:n:t:k:b:w:K:V:s:r:m:vfS")) != -1) 
	{
		switch (c)
		{
		case 'h':
			g_config.host = strdup(optarg);
			break;
		
		case 'p':
			g_config.port = atoi(optarg);
			if (g_config.port <= 0) {
				fprintf(stderr, "port must be a number\n");
				usage();
				return(0);
			}
			break;

		case 'H':
			g_config.host = malloc(strlen(optarg));
		    char *tp = optarg;
		    while (*tp++) {
		    	if (*tp == ':') break;
		    }
		    if (*tp == ':') *tp = 0;
		    strcpy(g_config.host, optarg);
		    g_config.port = atoi(tp + 1);
		    fprintf(stderr, " host %s port %d\n",g_config.host,g_config.port);
			break;

		case 'n':
			g_config.ns = strdup(optarg);
			break;

		case 's':
			g_config.set = strdup(optarg);
			break;

		case 'b':
			g_config.bin = strdup(optarg);
			break;

		case 't':
			g_config.n_threads = atoi(optarg);
			break;
			
		case 'k':
			g_config.n_keys = atoi(optarg);
			break;


		case 'K':
			g_config.key_len = atoi(optarg);
			break;
			
		case 'V':
			g_config.value_len = atoi(optarg);
			break;

		case 'r':
			g_config.pseudo_seed = atoi(optarg);
			break;
			
		case 'm':
			g_config.timeout_ms = atoi(optarg);
			break;
			
		case 'v':
			g_config.verbose = true;
			break;

		case 'f':
			g_config.follow = false;
			break;

		case 'S':
			g_config.follow = false;
			break;
			
		default:
			usage();
			return(-1);
			
		}
	}
	fprintf(stderr, "testing: host %s port %d ns %s set %s bin %s\n",g_config.host,g_config.port,g_config.ns,g_config.set,g_config.bin);

	if (g_config.pseudo_seed)
		srandom(g_config.pseudo_seed);
	
	citrusleaf_init();

	fprintf(stderr, "key_test: keys: %d threads: %d \n",g_config.n_keys,g_config.n_threads);

	if (0 != do_loop_test()) {
		fprintf(stderr, "could not init test!\n");
		return(-1);
	}

	return(0);
}
