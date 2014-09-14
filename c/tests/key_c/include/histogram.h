/*
 *	histogram.h
 *
 *	Histogram generator for Citrusleaf drive IO simulator
 *	Joey Shurtleff & Andrew Gooding, 2011
 *	Copyright, all rights reserved
 */

#pragma once

#include <inttypes.h>
#include "citrusleaf/cf_atomic.h"

#define N_COUNTS 64

typedef struct _histogram {
	cf_atomic_int n_counts;
	cf_atomic_int count[N_COUNTS];
} histogram;

extern histogram* histogram_create();
extern void histogram_dump(histogram* h, const char* p_tag);
extern void histogram_insert_data_point(histogram* h, uint64_t delta_ms);
