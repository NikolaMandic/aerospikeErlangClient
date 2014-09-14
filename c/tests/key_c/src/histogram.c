/*
 *	histogram.c
 *
 *	Histogram generator for Citrusleaf drive IO simulator
 *	Joey Shurtleff & Andrew Gooding, 2011
 *	Copyright, all rights reserved
 */


//==========================================================
// Includes
//

#include "histogram.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

//#include "atomic.h"
#include "clock.h"


//==========================================================
// Constants
//

const char LogTable256[] = {
#define LT(n) n, n, n, n, n, n, n, n, n, n, n, n, n, n, n, n
	-1, 0, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3,
	LT(4), LT(5), LT(5), LT(6), LT(6), LT(6), LT(6),
	LT(7), LT(7), LT(7), LT(7), LT(7), LT(7), LT(7), LT(7)
};


//==========================================================
// Forward Declarations
//

// Renamed for clarity and to match 'bits_find_last_set' utilities:
#define bits_find_first_set_64(__x) ffsll(__x)

static int bits_find_last_set(uint32_t v);
static int bits_find_last_set_64(uint64_t v);


//==========================================================
// Public API
//

histogram* histogram_create() {
	histogram* h = malloc(sizeof(histogram));

	if (h) {
		h->n_counts = 0;
		memset(&h->count, 0, sizeof(h->count));
	}

	return (h);
}

void histogram_dump(histogram* h, const char* p_tag) {
	char printbuf[100];
	int pos = 0; // location to print from
	printbuf[0] = '\0';

	fprintf(stdout, "%s (%zu total)\n", p_tag, h->n_counts);

	int i, j;
	int k = 0;

	for (j = N_COUNTS-1 ; j >= 0 ; j--) {
		if (h->count[j]) break;
	}

	for (i = 0; i < N_COUNTS; i++) {
		if (h->count[i]) break;
	}

	for (; i <= j; i++) {
		if (h->count[i] > 0) { // print only non zero columns
			int bytes = sprintf(
				(char*)(printbuf + pos), " (%02d: %010zu) ", i, h->count[i]);

			if (bytes <= 0) {
				fprintf(stdout, "ERROR: printing histogram\n");
				return;
			}

			pos += bytes;

		    if (k % 4 == 3) {
		    	 fprintf(stdout, "%s\n", (char*)printbuf);
		    	 pos = 0;
		    	 printbuf[0] = '\0';
		    }

		    k++;
		}
	}

	if (pos > 0) {
	    fprintf(stdout, "%s\n", (char*)printbuf);
	}
}

void histogram_insert_data_point(histogram* h, uint64_t delta_ms) {
	cf_atomic_int_incr(&h->n_counts);

	int index = bits_find_last_set_64(delta_ms);

	if (index < 0) {
		index = 0;
	}

	if ((int64_t)delta_ms < 0) {
	    // Need to investigate why in some cases start is a couple of ms greater than end
		// Could it be rounding error (usually the difference is 1 but sometimes I have seen 2
	    // fprintf(stdout, "start = %"PRIu64" > end = %"PRIu64"", start, end);
		index = 0;
	}

	cf_atomic_int_incr(&h->count[index]);
}


//==========================================================
// Utilities
//

static int bits_find_last_set(uint32_t v) {
	int r;
	uint32_t t, tt;

	if ((tt = v >> 16) != 0) {
		r = (t = tt >> 8) ? (24 + LogTable256[t]) : (16 + LogTable256[tt]);
	}
	else {
		r = (t = v >> 8) ? (8 + LogTable256[t]) : LogTable256[v];
	}

	return (r);
}

static int bits_find_last_set_64(uint64_t v) {
	uint64_t t;

	if ((t = v >> 32) != 0) {
		return (bits_find_last_set(t) + 32);
	}
	else {
		return (bits_find_last_set(v));
	}
}
