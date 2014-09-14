#include "citrusleaf/citrusleaf.h"
#include "citrusleaf/cl_shm.h"
#include "citrusleaf/cl_request.h"

// HOST_NAME_MAX should be defined in limits.h. But do this just in case
#ifndef HOST_NAME_MAX
#define HOST_NAME_MAX 255
#endif

// All GW error codes, except GW_OK, must be negative!
#define GW_OK 0
#define GW_CONNERROR_TOOMANY -1
#define GW_CONNERROR_CLUSTER_CREATE_FAIL -2
#define GW_CONNERROR_HANDLE_NOT_VALID -3
#define GW_ERROR_OUTOFMEMORY -4
#define GW_ERROR_HISTOGRAM_FAIL -5
#define GW_ERROR_STOPWATCH_SEQ -6

#define CONNECTION_MAX 64

// This limits are just to control the buffer size needed to declare.
// We will let the DB server decide the true limit. We just want to pass the data along whenever is possible
// know limits:  ns = 31, set = 63, binname=32 
#define NAMELEN_MAX 1024

// let the server returns CITRUSLEAF_FAIL_PARAMETER or similar on name size exceptions
#define NSNAME_MAX NAMELEN_MAX
#define SETNAME_MAX NAMELEN_MAX

// this is not the server limit, but limit in the cl_bin_s structure
#define BINNAME_MAX 32

typedef int GW_RC;
GW_RC gw_init();
GW_RC gw_connect(char *host, int port, int *con_h, int *cl_rc);
GW_RC gw_addhost(int con_h, char *host, int port, int timeout_ms, int *cl_rc);
GW_RC gw_shutdown(int con_h);
GW_RC gw_shutdown_all();
GW_RC validate_con_h(int con_h);
GW_RC gw_clinfo(int con_h, char* names, char **infovalues, int *cl_rc);
GW_RC gw_put(int con_h, char *ns, char *set, cl_object *key, cl_bin values[], int value_count, cl_write_parameters *cl_w_p, int *cl_rc);
GW_RC gw_get(int con_h, char *ns, char *set, cl_object *key, cl_bin *bins, int n_bins, int timeout_ms, int *cl_rc);
GW_RC gw_getall(int con_h, char *ns, char *set, cl_object *key, cl_bin **bins, int *n_bins, int timeout_ms, int *cl_rc);
GW_RC gw_delete(int con_h, char *ns, char *set, cl_object *key, cl_write_parameters *cl_w_p, int *cl_rc);
GW_RC gw_histogram_start();
GW_RC gw_histogram_report(time_t *time_lapsed, char *writereport, size_t w_len, char *readreport, size_t r_len);
GW_RC gw_stopwatch_start();
GW_RC gw_stopwatch_stop(time_t *time_lapsed, int *nwrite, int *nread);
GW_RC gw_stopwatch_report(time_t *time_lapsed, int *nwrite, int *nread);
