#include <unistd.h>
#include <string.h>
#include <malloc.h>
#include <time.h>
#include "citrusleaf/citrusleaf.h"
#include "citrusleaf/cf_hist.h"
#include "citrusleaf/cf_atomic.h"
#include "cl_client_gw.h"

/**
 * CL CLIENT GW -- the C Language "gateway" from the Erlang client to the
 * C Aerospike Server.  Erlang function calls are converted to C calls
 * via the "NIF" (Native Implemented Function) mechanism.  The
 * citrusleaf_nif.c file contains the interface structures and functions
 * that are invoked from the Erlang client.  That in turn calls this function
 * to perform the actual work in C.
 * (Jan 2013 tjl)
 */

/**
 * This is the config file that holds the needed information for communicating
 * with the ASD (CLD) Server.
 */
// note: HOST_NAME_MAX comes from limits.h
typedef struct config_s {
  char  host[1+HOST_NAME_MAX]; // Name of the host running the ASD Server
  int   port;   // Port (default 3000) for the ASD Server
  char *ns;   // Namespace used (default is "test")
  char *set;  // Name of the Set (default is "")
  int   timeout_ms; // Timeout in milliseconds
  cl_cluster      *asc;
} config;

static int g_last_conn_index = -1;
static config *g_config[CONNECTION_MAX];
static config g_config_place_holder;

static bool g_want_histogram = false;
static cf_histogram* g_read_histogram = NULL;
static cf_histogram* g_write_histogram = NULL;
static time_t g_histogram_starttime = 0;

static bool g_want_stopwatch = false;
static time_t g_stopwatch_starttime = 0;
static time_t g_stopwatch_stoptime = 0;
static cf_atomic_int g_nwrite = 0;
static cf_atomic_int g_nread = 0;

// This needs to be added to appropriate the .h file(s)
extern void
cf_histogram_dump_new( cf_histogram *h, char *outbuff, size_t outbuff_len);

// Define to print debug messages:  (This should be set by MAKE)
// This should be turned on/off by MAKE
// ===============================================
// #define DEBUG // turned on maually (for now)
// ===============================================
//
// If DEBUG is on, then we'll use our own compile time switch to do some
// print statements that should be compiled out ( if(false) ) when the
// flag is off.  Actually -- even though the prints have now been converted
// to the system trace/debug calls (cf_info(), cf_debug()), ...) we'll still
// use the ifdefs to remove some of the debug statements that might be in
// high traffic areas.
// (Jan 2013 tjl)
//
//  <<< Ok -- sometimes we have to turn if OFF even when Make turns it on >>>
// #ifdef DEBUG
// #undef DEBUG
// #endif

#ifdef DEBUG // --------------------------------
// Enter and Exit Debug prints
#define TRA_ENTER true
#define TRA_EXIT true
// Error and Warning Debug prints
#define TRA_ERROR true
// Informational Debug prints
#define TRA_INFO true
// MAXIMUM verbosity  Debug prints
#define TRA_DEBUG true
#else // --------------------------------
// Enter and Exit Debug prints
#define TRA_ENTER false
#define TRA_EXIT false
// Error and Warning Debug prints
#define TRA_ERROR false
// Informational Debug prints
#define TRA_INFO false
// MAXIMUM verbosity  Debug prints
#define TRA_DEBUG false
#endif // --------------------------------

// Use these to help print out tracing stuff.
#define D_ENTER "=> ENTER:"
#define D_EXIT  "<= EXIT:"
#define D_INFO  "<I> INFO:"
#define D_DEBUG "<D> DEBUG:"
#define D_ERROR ">E< ERROR:"
#define D_WARN  ">W< WARNING:"

/**
 * Define the Mutex that protects our connection state structure (below).
 * The assigning of connection IDs (with physical storage)
 * has to be atomic - so set a mutex here to ensure that only one
 * thread can be in here at a time.
 */
pthread_mutex_t g_conn_index_LOCK = PTHREAD_MUTEX_INITIALIZER;

/**
 *
 */
int get_next_conn_index() {

  // The assigning of connection IDs (with physical storage)
  // has to be atomic - so set a mutex here to ensure that only one
  // thread can be in here at a time.
  // NOTE: As a result of this set -- this code MUST not return early.
  // All exits of this method must release the mutex.
  pthread_mutex_lock(&g_conn_index_LOCK);

  static char * meth = "get_next_conn_index()";
  if( TRA_ENTER )
    fprintf(stderr,"%s[%s]:   \n", D_ENTER, meth  );

  // first check the rest of the list for the first free slot
  int i;
  int found = 0;
  for (i = g_last_conn_index + 1; i < CONNECTION_MAX; i++) {
    if (NULL == g_config[i]) {
      found = -1;
      g_last_conn_index = i;
      break;
    }
  } // end for each connection.

  // last ditch effor, go thru the whole list from the beginning
  if (!found) {
    for (i = 0; i < CONNECTION_MAX; i++) {
      if (NULL== g_config[i]) {
        found = -1;
        g_last_conn_index = i;
        break;
      }
    } 
  }
  // mark the occupancy with a place holder
  if (found) {
    g_config[g_last_conn_index] = &g_config_place_holder;
  }

// Done allocating the connection info -- now release the mutex.
  pthread_mutex_unlock(&g_conn_index_LOCK);

  if (found) {
    return g_last_conn_index;
  } else {
    return -1;
  }
} // end get_next_conn_index()

/**
 *  Validate that the connection ID is in the right range.
 */
GW_RC validate_con_h(int con_h) {
  static char * meth = "validate_con_h()";
  if( TRA_ENTER )
    fprintf(stderr,"%s[%s]: con_h(%d)  \n", D_ENTER, meth, con_h );

  if (0 > con_h) {
    if(TRA_ERROR) fprintf(stderr,"%s[%s]: Neg handle\n",D_ERROR,meth);
    return GW_CONNERROR_HANDLE_NOT_VALID;
  }
  if (CONNECTION_MAX <= con_h) {
    if(TRA_ERROR) fprintf(stderr,"%s[%s]: Ch(%d) > Max(%d)\n",
      D_ERROR,meth, con_h, CONNECTION_MAX );
    return GW_CONNERROR_HANDLE_NOT_VALID;
  }
  if (NULL == g_config[con_h]) {
    if(TRA_ERROR) fprintf(stderr,"%s[%s]: No Entry Present for ConH(%d_\n",
        D_ERROR,meth, con_h );
    return GW_CONNERROR_HANDLE_NOT_VALID;
  }
  return GW_OK;
} // end validate_con_h()

/**
 * Initialize our connection data structure.
 */
GW_RC gw_init() {
  static char * meth = "gw_init()";
  if( TRA_ENTER )
    fprintf(stderr,"%s[%s]:   \n", D_ENTER, meth  );

  citrusleaf_init();
  int i;
  for (i = 0; i < CONNECTION_MAX; i++) {
    g_config[i] = NULL;
  }

  g_read_histogram = NULL;
  g_write_histogram = NULL;

  return GW_OK;
} // end gw_init()

/**
 * Connect to an Aerospike (ASD) / Citrusleaf (CLD) Cluster.
 * This method takes in Host and Port, and uses that to create a new
 * entry in the connection data structure and then connect to the server.
 *
 * Note that there are two different return codes -- one is for the gateway
 * function (GW RC), which returns "error" up to the client if something
 * really bad happens (usually, internally), and the other is for the
 * Citrusleaf Client function (CL RC), which returns "citrusleaf_error".
 * So, if nothing really terrible happens in the GW function, it always
 * returns GW_OK, and any citrusleaf error code is passed along.
 * If something really bad happens, then the citrusleaf code is ignored
 * and the internal "error" is passed up.
 */
GW_RC gw_connect(char *host, int port, int *con_h, int *cl_rc) {
  static char * meth = "gw_connect()";

  if( TRA_ENTER ) fprintf(stderr,"%s[%s]: Host(%s) Port(%d) \n",
        D_ENTER, meth, host, port );

  if(TRA_DEBUG) fprintf(stderr,"%s[%s]:calling get next conn index\n",
      D_DEBUG, meth );
  int my_con_h = get_next_conn_index();
  if(TRA_DEBUG) fprintf(stderr,"%s[%s]:conn index, gets(%d)\n",
      D_DEBUG, meth, my_con_h );

  if (my_con_h < 0  ) {
    // could not get a free handle space
    if( TRA_ERROR ) fprintf(stderr,"%s[%s]: Too Many Handles: Max(%d)\n",
          D_ERROR, meth, CONNECTION_MAX );

    return (GW_CONNERROR_TOOMANY);
  }

  config *my_config = (config *)malloc(sizeof(config));
  if (!my_config) {
    return (GW_ERROR_OUTOFMEMORY);
  }

  if( TRA_DEBUG ) fprintf(stderr,"%s[%s]: About to create cluster.\n",
      D_DEBUG, meth );
  my_config->asc = citrusleaf_cluster_create();
  if (!(my_config->asc)) {
    if( TRA_ERROR ) fprintf(stderr,"%s[%s]: Could not create Cluster.\n",
          D_ERROR, meth );
    free(my_config);
    return (GW_CONNERROR_CLUSTER_CREATE_FAIL);
  }
  strncpy(my_config->host, host, HOST_NAME_MAX);
  my_config->port = port;
  my_config->ns = NULL;
  my_config->set = NULL;
  my_config->timeout_ms = 100;

  if( TRA_DEBUG ) fprintf(stderr,"%s[%s]: About to add host.\n",D_DEBUG, meth);
  *cl_rc = citrusleaf_cluster_add_host(my_config->asc, my_config->host,
      my_config->port, my_config->timeout_ms);
  if (CITRUSLEAF_OK != *cl_rc) {
    g_config[my_con_h] = NULL;       // signal the slot is free for recycling
    free(my_config);
    *con_h = -1;
    if( TRA_ERROR ) fprintf(stderr,"%s[%s]: Error adding host(%s)\n",
        D_ERROR, meth, host );
    // we can return GW_OK here since cl_rc contains the real citrusleaf
    // error code at this point
    return GW_OK;
  } // end cluster add failed.

  g_config[my_con_h] = my_config;

  // Since add_host will always succeed with CITRUSLEAF_OK even if the
  // host/port is not there, we'll verify the connection is OK first before
  // returning to the caller with OK
  if( TRA_DEBUG ) fprintf(stderr,"%s[%s]: Testing CL INFO.\n",D_DEBUG, meth);
  char *clinfo;
  if (GW_OK != gw_clinfo(my_con_h, "status", &clinfo, cl_rc)) {
    g_config[my_con_h] = NULL;
    free(my_config);
    *con_h = -1;
    if(TRA_ERROR) fprintf(stderr,"%s[%s]: clinfo fail(%s)\n",D_ERROR, meth );
    return GW_CONNERROR_CLUSTER_CREATE_FAIL;
  }

  // Good. We got a reply from the cluster
  if ((clinfo) && (0 == strncmp(clinfo, "status", 6))) {
    *con_h = my_con_h;
  } else {
    g_config[my_con_h] = NULL;
    free(my_config);
    *con_h = -1;
  }

  // returning GW_OK since cl_rc contains the real citrusleaf error
  // code at this point
  if( TRA_EXIT ) fprintf(stderr,"%s[%s]: GW RC(%d) CL RC(%d)\n",
      D_EXIT, meth, GW_OK, *cl_rc );

  return GW_OK;
} // end gw_connect()

/**
 * Add a host to the cluster.
 *
 * Note that there are two different return codes -- one is for the gateway
 * function (GW RC), which returns "error" up to the client if something
 * really bad happens (usually, internally), and the other is for the
 * Citrusleaf Client function (CL RC), which returns "citrusleaf_error".
 * So, if nothing really terrible happens in the GW function, it always
 * returns GW_OK, and any citrusleaf error code is passed along.
 * If something really bad happens, then the citrusleaf code is ignored
 * and the internal "error" is passed up.
 */
GW_RC gw_addhost(int con_h, char *host, int port, int timeout_ms, int *cl_rc)
{
  static char * meth = "gw_addhost()";
  if( TRA_ENTER )
    fprintf(stderr,"%s[%s]:   \n", D_ENTER, meth  );

  GW_RC con_validate_rc =  validate_con_h(con_h);
  if (GW_OK != con_validate_rc) {
    return con_validate_rc;
  }

  config *my_config = g_config[con_h];
  *cl_rc =  citrusleaf_cluster_add_host(my_config->asc, host, port, timeout_ms);
  return (GW_OK);
} // end gw_addhost()

/**
 * Shutdown this connection.
 *
 * Note that there are two different return codes -- one is for the gateway
 * function (GW RC), which returns "error" up to the client if something
 * really bad happens (usually, internally), and the other is for the
 * Citrusleaf Client function (CL RC), which returns "citrusleaf_error".
 * So, if nothing really terrible happens in the GW function, it always
 * returns GW_OK, and any citrusleaf error code is passed along.
 * If something really bad happens, then the citrusleaf code is ignored
 * and the internal "error" is passed up.
 */
GW_RC gw_shutdown(int con_h) {
  static char * meth = "gw_shutdown()";
  if( TRA_ENTER )
    fprintf(stderr,"%s[%s]:   \n", D_ENTER, meth  );

  GW_RC con_validate_rc =  validate_con_h(con_h);
  if (GW_OK != con_validate_rc) {
    return con_validate_rc;
  }
  config *my_config = g_config[con_h];
  citrusleaf_cluster_destroy(my_config->asc);
  my_config->asc = NULL;
  free(my_config);

  // the handle can now be recycled
  g_config[con_h] = NULL;

  return GW_OK;
} // end gw_shutdown()

/**
 * Shutdown all of the connections
 */
GW_RC gw_shutdown_all()
{
  static char * meth = "gw_shutdown_all()";
  if( TRA_ENTER )
    fprintf(stderr,"%s[%s]:   \n", D_ENTER, meth  );

  // clean up config table
  int i;
  for (i=0; i<CONNECTION_MAX; i++) {
    if (g_config[i]) {
      citrusleaf_cluster_destroy(g_config[i]->asc);
      free(g_config[i]);

      // recycle the handle
      g_config[i] = NULL;
    }
  } // end for
  // ready to reuse all the handles
  g_last_conn_index = -1;

  // No point of getting rid of all citrusleaf infrastructure.
  // User may connect again!
  // citrusleaf_shutdown();

  return GW_OK;
} // end gw_shutdown_all()

/**
 * Return the Citrusleaf Information.
 *
 * Note that there are two different return codes -- one is for the gateway
 * function (GW RC), which returns "error" up to the client if something
 * really bad happens (usually, internally), and the other is for the
 * Citrusleaf Client function (CL RC), which returns "citrusleaf_error".
 * So, if nothing really terrible happens in the GW function, it always
 * returns GW_OK, and any citrusleaf error code is passed along.
 * If something really bad happens, then the citrusleaf code is ignored
 * and the internal "error" is passed up.
 */
GW_RC gw_clinfo(int con_h, char *names, char **infovalues, int *cl_rc)
{
  static char * meth = "gw_clinfo()";

  if( TRA_ENTER )
    fprintf(stderr,"%s[%s]:   \n", D_ENTER, meth  );

  GW_RC con_validate_rc =  validate_con_h(con_h);
  if (GW_OK != con_validate_rc) {
    return con_validate_rc;
  }

  config *my_config = g_config[con_h];
  *infovalues = NULL;

  *cl_rc =
    citrusleaf_info(my_config->host, my_config->port, names, infovalues, 0);

  return GW_OK;
} // end gw_clinfo()

/**
 * Write (put) to the Aerospike Server.  Call the C Citrusleaf Client to
 * perform the write.
 *
 * Note that there are two different return codes -- one is for the gateway
 * function (GW RC), which returns "error" up to the client if something
 * really bad happens (usually, internally), and the other is for the
 * Citrusleaf Client function (CL RC), which returns "citrusleaf_error".
 * So, if nothing really terrible happens in the GW function, it always
 * returns GW_OK, and any citrusleaf error code is passed along.
 * If something really bad happens, then the citrusleaf code is ignored
 * and the internal "error" is passed up.
 */
GW_RC gw_put(int con_h, char *ns, char *set, cl_object *key, cl_bin values[],
    int value_count, cl_write_parameters *cl_w_p, int *cl_rc)
{
  static char * meth = "gw_put()";
  if( TRA_ENTER )
    fprintf(stderr,"%s[%s]:   \n", D_ENTER, meth  );

  GW_RC con_validate_rc =  validate_con_h(con_h);
  if (GW_OK != con_validate_rc) {
    if( TRA_ERROR )
      fprintf(stderr,"%s[%s]:ERROR(%d) \n", D_ENTER, meth, con_validate_rc );
    return con_validate_rc;
  }

  config *my_config = g_config[con_h];

  uint64_t start_time = 0;
  if (g_want_histogram) {
    start_time = cf_getms();
  }

  // Now we're including the Write Options (Jan 2013: tjl)
  *cl_rc =
    citrusleaf_put(my_config->asc, ns, set, key, values, value_count, cl_w_p);

  if (start_time)
    cf_histogram_insert_data_point(g_write_histogram, start_time);

  if(TRA_DEBUG) {
    fprintf(stderr,"%s[%s]: Put Results(%d) <><><> g_nwrite before PUT(%d)",
        D_DEBUG, meth, *cl_rc, g_nwrite );
  }

  if (g_want_stopwatch)
    cf_atomic_int_incr(&g_nwrite);

  if(TRA_DEBUG)
    fprintf(stderr,":::  g_nwrite after PUT(%d)(<><><> \n", g_nwrite );

  return GW_OK;
} // end gw_put()

/**
 * Get the specified bins from the record that matches this specific key.
 * Perform the query, get the values from the C Citrusleaf client.
 *
 * Note that there are two different return codes -- one is for the gateway
 * function (GW RC), which returns "error" up to the client if something
 * really bad happens (usually, internally), and the other is for the
 * Citrusleaf Client function (CL RC), which returns "citrusleaf_error".
 * So, if nothing really terrible happens in the GW function, it always
 * returns GW_OK, and any citrusleaf error code is passed along.
 * If something really bad happens, then the citrusleaf code is ignored
 * and the internal "error" is passed up.
 */
GW_RC gw_get(int con_h, char *ns, char *set, cl_object *key,  cl_bin *bins,
    int n_bins, int timeout_ms, int *cl_rc)
{
  static char * meth = "gw_get()";
  if( TRA_ENTER )
    fprintf(stderr,"%s[%s]:   \n", D_ENTER, meth  );

  GW_RC con_validate_rc =  validate_con_h(con_h);
  if (GW_OK != con_validate_rc) {
    return con_validate_rc;
  }

  config *my_config = g_config[con_h];

  int i;
  for (i = 0; i < n_bins; i++) {
    citrusleaf_object_init(&bins[i].object);
  }

  uint64_t start_time = 0;
  if (g_want_histogram) {
    start_time = cf_getms();
  }

  // we are ignoring the gen_count, for now
  *cl_rc = citrusleaf_get(my_config->asc, ns, set, key, bins, n_bins,
      timeout_ms, NULL);

  if(TRA_DEBUG) fprintf(stderr,"<><><> g_nread before GET(%d)", g_nread );

  if (start_time)
    cf_histogram_insert_data_point(g_read_histogram, start_time);
  if (g_want_stopwatch)
    cf_atomic_int_incr(&g_nread);

 if(TRA_DEBUG) fprintf(stderr,":::  g_nread after GET(%d)<><><> \n", g_nread );

  return GW_OK;
} // end gw_get()

/**
 * Get ALL of the bins from the record that corresponds to this specific key.
 * Query the Citrusleaf C Client.
 *
 * Note that there are two different return codes -- one is for the gateway
 * function (GW RC), which returns "error" up to the client if something
 * really bad happens (usually, internally), and the other is for the
 * Citrusleaf Client function (CL RC), which returns "citrusleaf_error".
 * So, if nothing really terrible happens in the GW function, it always
 * returns GW_OK, and any citrusleaf error code is passed along.
 * If something really bad happens, then the citrusleaf code is ignored
 * and the internal "error" is passed up.
 */
GW_RC gw_getall(int con_h, char *ns, char *set, cl_object *key,
    cl_bin **bins, int *n_bins, int timeout_ms, int *cl_rc)
{
  static char * meth = "gw_getall()";
  if( TRA_ENTER )
    fprintf(stderr,"%s[%s]:   \n", D_ENTER, meth  );

  GW_RC con_validate_rc =  validate_con_h(con_h);
  if (GW_OK != con_validate_rc) {
    if( TRA_DEBUG ) fprintf(stderr, "%s[%s]: Couldn't validate C(%d)\n",
        D_DEBUG, meth, con_h );
    return con_validate_rc;
  }

  config *my_config = g_config[con_h];

  uint64_t start_time = 0;
  if (g_want_histogram) {
    start_time = cf_getms();
  }

  if( TRA_DEBUG ) fprintf(stderr, "%s[%s]: Calling getall() \n",
      D_DEBUG, meth, con_h );

  *cl_rc = citrusleaf_get_all(my_config->asc, ns, set, key, bins, n_bins,
  timeout_ms, NULL);

  if(TRA_DEBUG) fprintf(stderr,"<><><> g_nread before GETALL(%d)", g_nread );

  if (start_time)
    cf_histogram_insert_data_point(g_read_histogram, start_time);
  if (g_want_stopwatch)
    cf_atomic_int_incr(&g_nread);

 if(TRA_DEBUG)
   fprintf(stderr,":::  g_nread after GETALL(%d)<><><> \n", g_nread );

  return GW_OK;
} // end gw_getall()

/**
 * Delete the record that matches this key.
 *
 * Note that there are two different return codes -- one is for the gateway
 * function (GW RC), which returns "error" up to the client if something
 * really bad happens (usually, internally), and the other is for the
 * Citrusleaf Client function (CL RC), which returns "citrusleaf_error".
 * So, if nothing really terrible happens in the GW function, it always
 * returns GW_OK, and any citrusleaf error code is passed along.
 * If something really bad happens, then the citrusleaf code is ignored
 * and the internal "error" is passed up.
 */
GW_RC gw_delete(int con_h, char *ns, char *set, cl_object *key,
    cl_write_parameters *cl_w_p, int *cl_rc)
{
  static char * meth = "gw_delete()";
  if( TRA_ENTER ) fprintf(stderr,"%s[%s]: con_h(%d)\n", D_ENTER, meth,con_h );

  GW_RC con_validate_rc =  validate_con_h(con_h);
  if (GW_OK != con_validate_rc) {
    return con_validate_rc;
  }
  if( TRA_DEBUG ) fprintf(stderr,"%s[%s]:Validate OK  \n", D_DEBUG, meth  );

  // TODO add support of cl_w_p
  config *my_config = g_config[con_h];

  uint64_t start_time = 0;
  if (g_want_histogram) {
    start_time = cf_getms();
  }

  if( TRA_DEBUG ) fprintf(stderr,"%s[%s]:Calling CL Del  \n", D_DEBUG, meth  );
  // Make the call that does the real work.  NOtice that we are now using
  // the Citrusleaf Write Parameters.
  *cl_rc = citrusleaf_delete(my_config->asc, ns, set, key, cl_w_p );
  if( TRA_DEBUG ) fprintf(stderr,"%s[%s]:Del:rc(%d)\n", D_DEBUG, meth,*cl_rc );

if(TRA_DEBUG) fprintf(stderr,"<><><> g_nwrite before DELETE(%d)", g_nwrite );

  if (start_time)
    cf_histogram_insert_data_point(g_write_histogram, start_time);
  if (g_want_stopwatch)
    cf_atomic_int_incr(&g_nwrite);

  if(TRA_DEBUG)
    fprintf(stderr,":::  g_nwrite after DELETE(%d)<><><> \n", g_nwrite );

  return GW_OK;
} // end gw_delete()

/**
 * Start the histogram time and counters
 */
GW_RC gw_histogram_start() {
  static char * meth = "gw_histogram_start()";
  if( TRA_ENTER )
    fprintf(stderr,"%s[%s]:   \n", D_ENTER, meth  );

  if (g_read_histogram==NULL) {
    g_read_histogram = cf_histogram_create("r_gw");
    g_write_histogram = cf_histogram_create("w_gw");

    if (g_read_histogram==NULL || g_write_histogram==NULL) {
      return GW_ERROR_HISTOGRAM_FAIL;
    }
  }

  // TODO add functionality to reset the histograms if they already exist

  g_want_histogram = true;
  g_histogram_starttime = time(0);
  return GW_OK;
} // end gw_histogram_start()

/**
 * Capture the histogram numbers and write them into the supplied buffer
 */
GW_RC gw_histogram_report(time_t *time_lapsed, char *w_report, size_t w_len,
    char *r_report, size_t r_len)
{
  static char * meth = "gw_histogram_report()";
  if( TRA_ENTER )
    fprintf(stderr,"%s[%s]:   \n", D_ENTER, meth  );

  *time_lapsed = time(0) - g_histogram_starttime;
  if (!*time_lapsed) {
    // in case too short a time for unix time()
    *time_lapsed = 1;
  }
  // New version of histogram dump that passes in storage to hold the report.
  cf_histogram_dump_new(g_write_histogram, w_report, w_len);
  cf_histogram_dump_new(g_read_histogram, r_report, r_len);

  return GW_OK;
} // end gw_histogram_report()

/**
 *
 */
GW_RC gw_stopwatch_start() {
  static char * meth = "gw_stopwatch_start()";
  if( TRA_ENTER )
    fprintf(stderr,"%s[%s]:   \n", D_ENTER, meth  );

  if(TRA_DEBUG)
   fprintf(stderr,"!!!!!!!!!! Starting Stopwatch !!!!!!!!!!!!! \n");

  // Must reset the counters when we start the clock.
  // It should be ok to do this -- if the stopwatch is turned off.
  g_want_stopwatch = false;
  g_nwrite = 0;
  g_nread = 0;

  // Ok -- now we start.
        g_want_stopwatch = true;
        g_stopwatch_starttime = time(0);
        return GW_OK;
} // end gw_stopwatch_start()

/**
 *
 */
GW_RC gw_stopwatch_stop(time_t *time_lapsed, int *nwrite, int *nread) {
  static char * meth = "gw_stopwatch_stop()";
  if( TRA_ENTER )
    fprintf(stderr,"%s[%s]:   \n", D_ENTER, meth  );

  if(TRA_DEBUG)
    fprintf(stderr,">>>>>>>>>> STOPPING  Stopwatch <<<<<<<<<<<<< \n");

    if (!g_want_stopwatch) {
            return GW_ERROR_STOPWATCH_SEQ;
    }

    g_stopwatch_stoptime = time(0);
    g_want_stopwatch = 0;
    return gw_stopwatch_report(time_lapsed, nwrite, nread);
} // end gw_stopwatch_stop()

/**
 *
 */
GW_RC gw_stopwatch_report(time_t *time_lapsed, int *nwrite, int *nread) {
        time_t end_time;
  static char * meth = "gw_stopwatch_report()";
  if( TRA_ENTER )
    fprintf(stderr,"%s[%s]:   \n", D_ENTER, meth  );


        // cannot report if the stopwatch never started
        if (!g_stopwatch_starttime) {
                return GW_ERROR_STOPWATCH_SEQ;
        }

        // if the stopwatch is still running, then report the lapsed time of
  // this moment
        if (g_want_stopwatch) {
                end_time = time(0);
        }
        else {
                end_time = g_stopwatch_stoptime;
        }

        *time_lapsed = end_time - g_stopwatch_starttime;
        if (!*time_lapsed) {
                // in case too short a time for unix time()
                *time_lapsed = 1;
        }
        *nwrite = g_nwrite;
        *nread = g_nread;
 if(TRA_DEBUG) fprintf(stderr," !!!!>>> Report( NumWrites(%d) NumReads(%d)\n",
       *nwrite, *nread );

        return GW_OK;
} // end GW_RC gw_stopwatch_report()

// <<<<<<<<<<<<<<<<<<<<<<<<<<<< END OF FILE >>>>>>>>>>>>>>>>>>>>>>>>>>>>

