// File aerospike_nif.c
#include <time.h>
#include <stdio.h>
#include <string.h>
#include "erl_nif.h"
#include "cl_client_gw.h"
// General Aerospike (was Citrusleaf) C Client structures
#include "citrusleaf/citrusleaf.h"

// Define to print debug messages:  (This should be set by MAKE)
// This should be turned on/off by MAKE
// ================================================
// #define DEBUG // turned on/off  maually (for now)
// ================================================

// If DEBUG is on, then we'll use our own compile time switch to do some
// print statements that should be compiled out ( if(false) ) when the
// flag is off.  Actually -- even though the prints have now been converted
// to the system trace/debug calls (cf_info(), cf_debug()), ...) we'll still
// use the ifdefs to remove some of the debug statements that might be in
// high traffic areas.
//
// <<< Ok -- sometimes we have to turn if OFF even when Make turns it on >>>
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
// REALLY BIG data dump
#define TRA_DETAIL false // !!!! Special Case:: Remember to change back !!!!!
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
// REALLY BIG data dump
#define TRA_DETAIL false 
#endif // --------------------------------

// Use these to help print out tracing stuff.
#define D_ENTER "=> ENTER:"
#define D_EXIT  "<= EXIT:"
#define D_INFO  "<I> INFO:"
#define D_DEBUG "<D> DEBUG:"
#define D_ERROR ">E< ERROR:"
#define D_WARN  ">W< WARNING:"

/**
 * External and Forward Definitions
 */
// Predeclare some of the functions (that need it):
int util_print_erlang_list(ErlNifEnv* envp, const ERL_NIF_TERM arg );
int util_return_as_type(ErlNifEnv* envp, const ERL_NIF_TERM arg );

/**
 * GLOBAL STATE
 */
// Track if we've initialized the Aerospike Client.
// Because of the design of NIF, we may end of trying multiple times, but
// we want to really apply that only once.
static int s_cl_init = 0; // Start out false

// NOTE: The Aerospike (was Citrusleaf) types defined here need to
// remain in sync with the C CLient enum types:
// ==> typedef enum cl_type cl_type;


/**
 * Since NIF is a little weird about how it handles types, this method
 * goes thru several different checks to see what type it is.
 * NIF doesn't allow us to look "into" the "black box" type.  Instead it lets
 * us * ask questions via functions (e.g. IsThisAnInt?()) and also it lets us
 * "TRY" to convert the black box type into something we understand
 * (e.g. len = MakeItAString()) and if len > 0, that was it.
 * One might ask -- why didn't they just give us a "what type is it()?"
 * function.  But then, that's not the Erlang way.
 * We're going to define our own TYPE constants for just this purpose,
 * but we'll make sure they line up with the CL TYPES where there's
 * an overlap (ER_CL_XXX is a Citrusleaf Type).
 */
#define ER_CL_NULL          0
#define ER_CL_INT           1
#define ER_CL_FLOAT         2
#define ER_CL_STR           3
#define ER_CL_BLOB          4
#define ER_CL_TIMESTAMP     5
#define ER_CL_DIGEST        6
#define ER_CL_JAVA_BLOB     7
#define ER_CL_CSHARP_BLOB   8
#define ER_CL_PYTHON_BLOB   9
#define ER_CL_RUBY_BLOB     10
#define ER_CL_PHP_BLOB      11
#define ER_CL_ERLANG_BLOB   12 
#define ER_XX_ATOM          13
#define ER_XX_LONG          14
#define ER_XX_UNSIGNED_LONG 15
#define ER_XX_EMPTY_LIST    16
#define ER_XX_LIST          17
#define ER_XX_RECORD        18
#define ER_XX_TUPLE         19
#define ER_XX_PORT          20
#define ER_XX_PID           21
#define ER_XX_REFERENCE     22
#define ER_XX_FUNCTION      23
#define ER_XX_MAX           24     // Upper Bound (and TYPE UNKNOWN)
#define ER_CL_UNKNOWN       666666 

// Associate Strings with the types defined above.
static char * g_erl_type_names[] = {
  "Type CL Null",                    //  0
  "Type CL Int",                     //  1
  "Type CL Float",                   //  2
  "Type CL STRING",                  //  3
  "Type CL Blob",                    //  4
  "Type CL Timestamp",               //  5
  "Type CL Digest",                  //  6
  "Type CL Java Blob",               //  7
  "Type CL CSharp Blob",             //  8
  "Type CL Python Blob",             //  9
  "Type CL Ruby Blob",               // 10
  "Type CL PHP Blob",                // 11
  "Type CL Erlang Blob",             // 12
  "Type Erlang Atom",                // 13
  "Type Erlang Long",                // 14
  "Type Erlang Unsigned Long",       // 15
  "Type Erlang EMPTY List",          // 16
  "Type Erlang List",                // 17
  "Type Erlang Record",              // 18
  "Type Erlang Tuple",               // 19
  "Type Erlang Port",                // 20
  "Type Erlang PID",                 // 21
  "Type Erlang Reference",           // 22
  "Type Erlang Function",            // 23
  "Type Unknown"                     // 24
};

/**
 * Return the string (from above) that lines up with the type number that
 * we're given.
 */
static char *
util_show_type_name( int er_type ){
  char * result;
  if( er_type >= 0 && er_type <= ER_XX_MAX ){
    result = g_erl_type_names[ er_type ];
  } else {
    result = g_erl_type_names[ ER_XX_MAX ]; // unknown.
  }
  fprintf(stderr, "Corresponding Name for Type#(%d) is {%s}\n",er_type,result);
  return( result );
}

/** (UTIL/DEBUG)
 * THis is good practice for processing Erlang Lists. 
 * For each element in the list, print out its type (and possibly value).
 */
int
util_print_erlang_list(ErlNifEnv* envp, const ERL_NIF_TERM arg ){
  int list_count = 0;
  int rc = 0;
  int i = 0;
  char workbuffer[4096]; // copy the list of cells into here
  ERL_NIF_TERM head; // List Processing:  Head
  ERL_NIF_TERM tail; // List Processing:  Tail
  static char * meth = "util_print_erlang_list()";

  rc = enif_get_list_length(envp, arg, &list_count);
  fprintf(stderr, "%s[%s]:List Count gets (%d) cells, with RC(%d)\n",
      D_DEBUG, meth, list_count, rc );

  // Iterate thru the cells -- print each one.
  if (( rc = enif_get_list_cell(envp, arg, &head, &tail)) == 0 ) {
    fprintf(stderr, "%s[%s]:Get List returns: RC(%d)\n", D_ERROR, meth, rc );
    return(-1);
  }

  // We might see several kinds of lists:
  // (1) Tuple Bin List:  [{bin1, val1}, {bin2, val2} ... ]
  // (2) Character Cell List ["A", "B", "C"]
  fprintf(stderr, "%s[%s]:>>>> SHOW LIST: total items(%d)<<<<\n",
      D_DEBUG, meth, list_count );
  for (i = 0; i < list_count; i++) {
    fprintf(stderr, "%s[%s]:>>>> LIST ITEM(%d)<<<<\n", D_DEBUG, meth, i );

    // Let's see what type the head is:
    util_return_as_type( envp, head );

    // move on to the next list member
    enif_get_list_cell(envp, tail, &head, &tail);

  } // end for each list item

} // end util_print_erlang_list()


/** (UTIL/DEBUG)
 * As a debug utility -- print out the numeric values of the bytes in
 * this buffer (up to 200)
 */
#define MAX_PRINT 200
#define NEW_LINE 20
void util_print_bytes( unsigned char * ptr, int size ){
  int item_count = 0;
  int max_count = size > MAX_PRINT? MAX_PRINT : size;
  while( item_count < MAX_PRINT ){
    printf(" %u,", *(ptr + item_count));
    if( (item_count % NEW_LINE) == 0 ) printf("\n");
    item_count++;
  } 
} // end util_print_bytes()

/** (UTIL/DEBUG)
 * Print the contents of an Erlang Tuple
 * The tuple must have a very specific structure/contents, otherwise it is
 * an error:
 */
int
util_print_erlang_tuple(ErlNifEnv* envp, const ERL_NIF_TERM arg )
{
  int arity;
  const ERL_NIF_TERM *tuple;
  ERL_NIF_TERM tuple_field;
  char str[32];
  int i = 0;
  int rc = 0;
  int  nChange = 0;

  static char * meth = "util_print_erlang_tuple()";
  if(TRA_ENTER) fprintf(stderr,"%s[%s]: \n",D_ENTER,meth );

  // Verify we actually have a tuple:  assumption is that ZERO (or less)
  // is an error, otherwise it is the "arity" of the tuple.
  if (( rc = enif_get_tuple(envp, arg, &arity, &tuple)) <= 0) {
    if(TRA_ERROR) fprintf(stderr,"%s[%s]:NOT A TUPLE!! rc(%d)\n",
      D_ERROR, meth, rc);
    return(-1);
  }

  // Iterate thru the tuple fields and show what we see
  fprintf(stderr, ">> SHOW TUPLE FIELDS <<n");
  for(i = 0; i < arity; i++ ){
    tuple_field = tuple[i];
    util_return_as_type( envp, tuple_field );
  } // end for each field in the tuple

  return(0);

} // end util_print_erlang_tuple()


// In the upcoming function, define our char/string workspace size
#define BUF_MAX 4096 // Our char string workspace size

/** (UTIL/DEBUG)
 * Given a single arg (from an array of ERL_NIF_TERM args given to the caller),
 * perform a series of tests on the Erlang term to determine what type
 * it is.
 * Note that this function probably can't be static, as multiple threads
 * will call it.
 */
int
util_return_as_type(ErlNifEnv* envp, const ERL_NIF_TERM arg ){
  char buffer[BUF_MAX]; // our string value workspace
  int intbuf;
  long longbuf;
  unsigned long u_longbuf;
  double doublebuf;
  int result = ER_CL_UNKNOWN;
  int len = 0;
  int item_count = 0;
  static char * meth = "util_return_as_type()";

  if( TRA_ENTER) fprintf(stderr, "%s[%s]: arg[%p] \n", D_ENTER, meth, arg );

  // Perform the individual tests on the <arg>.  As soon as we find
  // a match, break out of the test chain.
  // (*) Null Test ?  That might not be a type question.  Wait on this one.
  //
  // (*) Unsigned Int Test
  len = enif_get_uint( envp, arg, &intbuf );
  if( len > 0 ){
    if(TRA_DEBUG) fprintf(stderr,"%s[%s]: Found >> INT(%d):Len(%d)<<\n",
        D_DEBUG, meth, intbuf, len);
    result = ER_CL_INT;
    goto finish;
  }

  // (*) Long Test
  len = enif_get_long( envp, arg, &longbuf );
  if( len > 0 ){
    if(TRA_DEBUG) fprintf(stderr,"%s[%s]: Found >> LONG(%d): Len(%d)<< \n",
        D_DEBUG, meth, longbuf, len );
    result = ER_XX_LONG;  // No long type in CL
    goto finish;
  }

  // (*) UNSIGNED Long Test (really -- same as Long Test)
  len = enif_get_ulong( envp, arg, &u_longbuf );
  if( len > 0 ){
    if(TRA_DEBUG)
      fprintf(stderr,"%s[%s]: Found >> UNISGNED LONG(%u): Len(%d)<< \n",
        D_DEBUG, meth, u_longbuf, len );
    result = ER_XX_UNSIGNED_LONG; // there isn't an unsigned long type in CL
    goto finish;
  }

  // (*) Float/Double Test
  len = enif_get_double( envp, arg, &doublebuf );
  if( len > 0 ){
    if(TRA_DEBUG)
      fprintf(stderr,"%s[%s]: The Answer is >> DOUBLE(%f): Len(%d)<<\n",
        D_DEBUG, meth, doublebuf, len );
    result = ER_CL_FLOAT;
    goto finish;
  }

  // (*) String Test (more to learn about strings)
        len = enif_get_string(envp, arg, buffer, BUF_MAX, ERL_NIF_LATIN1);
  if( len > 0 ){
    if(TRA_DEBUG)
      fprintf(stderr,"%s[%s]: The answer is >> STRING(%s): Len(%d)<<\n",
        D_DEBUG, meth, buffer, len );
    result = ER_CL_STR;
    goto finish;
  }

  // (*) Blob Test (does not exist in NIF)
  // (*) TypeStamp Test? (does not exist in NIF)
  // (*) Digest Test? (does not exist in NIF)

  // (*) Erlang Atom Test
        if( enif_is_atom(envp, arg ) ) {
    if( TRA_DEBUG) fprintf(stderr, "I see an  >>> ATOM!!! <<<< \n");
  }
        len = enif_get_atom(envp, arg, buffer, BUF_MAX, ERL_NIF_LATIN1);
  if( len > 0 ){
    if(TRA_DEBUG)
      fprintf(stderr,"%s[%s]: The answer is >>>  ATOM(%s): Len(%d)<<<\n",
        D_DEBUG, meth, buffer, len );
    result = ER_XX_ATOM;
    goto finish;
  }

  // (*) Erlang Binary Test
  if( enif_is_binary( envp, arg ) ){
    if(TRA_DEBUG)
      fprintf(stderr, "The answer is >>> 1010 BINARY 0101 <<<< \n");
    result = ER_CL_BLOB;
    goto finish;
  }

  // (*) Erlang Empty List Test:
  if( enif_is_empty_list( envp, arg ) ){
    if(TRA_DEBUG)
      fprintf(stderr, "The answer is >>> [][] EMPTY LIST [][] <<<< \n");
    result = ER_XX_EMPTY_LIST;
    goto finish;
  }

  // (*) Erlang List Test:
  // Other List Operators:
  // ++ enif_get_list_cel()
  // ++ enif_get_list_length()
  if( enif_is_list( envp, arg ) ){
    if(TRA_DEBUG){
      len = enif_get_list_length ( envp, arg, &item_count );
      fprintf(stderr, "The answer is >>> [][] ERLANG LIST [%d][] <<<< \n",
          item_count);
      fprintf(stderr, ">>>>>  NOW PRINT LIST MEMBER TYPES <<<<<\n");
      util_print_erlang_list(  envp, arg );
    }
    result = ER_XX_LIST;
    goto finish;
  } // end if LIST

  // (*) Erlang Record Test: (Doesn't seem to exist ).  Perhaps Records
  // will show up as Tuples, if we ever see one.
  // (*) Erlang Tuple Test:
  if( enif_is_tuple( envp, arg ) ){
    if(TRA_DEBUG) {
      fprintf(stderr, "The answer is >>> []TT ERLANG TUPLE TT[] <<<< \n");
      util_print_erlang_tuple( envp, arg );
    }
    result = ER_XX_TUPLE;
    goto finish;
  } // end if TUPLE

  // Erlang Reference Test:
  if( enif_is_ref( envp, arg ) ){
    if(TRA_DEBUG)
      fprintf(stderr, "The answer is >>> [==> REFERENCE <==] <<<< \n");
    result = ER_XX_REFERENCE;
    goto finish;
  }

  // (*) Erlang Function Test:
  if( enif_is_fun( envp, arg ) ){
    if(TRA_DEBUG)
      fprintf(stderr, "The answer is >>> FUNCION(!!)  <<<< \n");
    result = ER_XX_FUNCTION;
    goto finish;
  }

  // Erlang PID Test:
  if( enif_is_pid( envp, arg ) ){
    if(TRA_DEBUG)
      fprintf(stderr, "The answer is >>> PROCESS_ID (=>00<=)  <<<< \n");
    result = ER_XX_PID;
    goto finish;
  }

  // Erlang PORT Test:
  if( enif_is_port( envp, arg ) ){
    if(TRA_DEBUG)
      fprintf(stderr, "The answer is >>> PORT (PPPPPP)  <<<< \n");
    result = ER_XX_PORT;
    goto finish;
  }

finish:
  if( TRA_EXIT ){
    fprintf(stderr, "%s[%s] Result(%d)\n", D_EXIT, meth, result );
    util_show_type_name( result );
  }

        return( result );
} // end util_return_as_type();

/** (UTIL/DEBUG)
 * Given an Erlang List of chars, convert that into a string and copy
 * it into the buffer supplied (checking length, just in case).
 * RETURN:
 * (*) > 0 (length of string) if OK
 * (*) <= 0 if error
 */
int
util_make_string_from_erlang_char_list(ErlNifEnv* envp,
    const ERL_NIF_TERM orig_list, char * buffer, int buf_len )
{
  ERL_NIF_TERM cell_list;
  ERL_NIF_TERM head;
  ERL_NIF_TERM tail;
  ERL_NIF_TERM *listptr;
  char * bufptr = buffer;
  int cell_val;
  int string_length = 0;
  int cellCounter = 0;
  int arg_type = 0;
  static char * meth = "util_make_string_from_erlang_char_list()";

  if( TRA_ENTER )
    fprintf(stderr, "%s[%s]: buffer[%p] BufLen(%d)\n",
        D_ENTER, meth, buffer, buf_len );

  listptr = (ERL_NIF_TERM *) &orig_list;

  while( enif_get_list_cell( envp, *listptr, &head, &tail )) {
    cellCounter++;

    // DEBUG -- check type of EACH cell.
    fprintf(stderr, "\n !!!!!!!<< CHECK TYPE OF LIST CELL >>!!!!!\n");
    arg_type = util_return_as_type( envp, head );
    fprintf(stderr, "\n\n:FFF:Function(%s): Cell#[%d] is type(%d){%s}\n",
        meth, cellCounter, arg_type, util_show_type_name( arg_type ) );

    if( enif_get_int( envp, head, &cell_val ) < 0 ){
      if( TRA_ERROR ) fprintf(stderr,"%s[%s]: Can't Read from Cell\n",
          D_ERROR, meth );
      return 0;
    } else {
      if( TRA_DEBUG ) fprintf(stderr,"%s[%s]: Read Int(%d){%c} from Cell\n",
          D_DEBUG, meth, cell_val, cell_val );
    }
    // Handle the "string too long" problem -- truncate quietly
    // Just copy "as much as fits" into the buffer and return.
    if( ++string_length > buf_len ){
      buffer[buf_len - 1] = 0; // Null terminate the string.
      return buf_len;
    }
    *bufptr = (char) cell_val;
    bufptr++; // move on to the next char position in the buffer
    listptr = &tail; // Erlang recursive style -- tail is "rest of list".
  } // end while each list cell
  *bufptr = 0; // Null Terminate the string

  if( TRA_EXIT )
    fprintf(stderr, "%s[%s]: Created String(%s)\n", D_EXIT, meth, buffer );

  return( string_length );

} // end util_make_string_from_erlang_char_list();


/** (UTIL/DEBUG)
 * Gather up all of the args and print their types.  This is a debug method.
 */
static void
util_show_all_args(char * caller, ErlNifEnv* envp, int argc,
    const ERL_NIF_TERM argv[])
{
  int i;
  int arg_type;
  for( i = 0; i < argc; i++ ){
    arg_type = util_return_as_type( envp, argv[i] );
    fprintf(stderr, "\n\n::Function(%s): Arg[%d] is type(%d){%s}\n",
        caller, i, arg_type, util_show_type_name( arg_type ) );
  } // end for each arg

} // end util_show_all_args()


/** (UTIL)
 * Make an error tuple from the description passed in.
 */
static ERL_NIF_TERM
util_make_error_tuple(ErlNifEnv* envp, char *desc) {
  static char * meth = "make_error_tuple()";
        if( TRA_ENTER)
                fprintf(stderr, "%s[%s]: desc(%s) \n", D_ENTER, meth, desc );

        return enif_make_tuple2(envp,
                                enif_make_atom(envp, "error"),
                                enif_make_string(envp, desc, ERL_NIF_LATIN1));
} // end util_make_error_tuple()

/** (UTIL)
 * Return the appropriate string for the GW Error Number
 */
static char*
util_lookup_gw_errmsg(GW_RC n)
{
  static char * meth = "util_lookup_gw_errmsg()";
  if( TRA_ENTER)
    fprintf(stderr, "%s[%s]: RC(%d) \n", D_ENTER, meth, n );

  // error code source is from cl_rv in citrusleaf.h
  switch (n) {
  case  GW_CONNERROR_TOOMANY: {
      return "Too many cluster connections";
      break;
  }
  case GW_CONNERROR_CLUSTER_CREATE_FAIL: {
      return "Failed to create an Aerospike cluster client";
      break;
  }
  case GW_CONNERROR_HANDLE_NOT_VALID: {
      return "Connection handle is not valid";
      break;
  }
  case GW_ERROR_OUTOFMEMORY: {
      return "Out of memory";
      break;
  }
  case GW_ERROR_HISTOGRAM_FAIL: {
      return "Failed to create gateway histograms";
      break;
  }
  case GW_ERROR_STOPWATCH_SEQ: {
      return "Stopwatch command sequence error";
      break;
  }
  case GW_OK: {
      return "ok";
      break;
  }
  default:      // should never see this
      return "Unknown gateway error";
      break;
  }
} // end utli_lookup_gw_errmsg()

/**
 * Return the appropriate string for the Aerospike (was Citrusleaf)
 * Client Error Number
 */
static char*
util_lookup_cl_errmsg(int n)
{
  static char * meth = "util_lookup_cl_errmsg()";
  if( TRA_ENTER) fprintf(stderr, "%s[%s]: RC(%d) \n", D_ENTER, meth, n );

  // error code source is from cl_rv in citrusleaf.h
  switch (n) {
    case CITRUSLEAF_FAIL_ASYNCQ_FULL: {
        return "CITRUSLEAF_FAIL_ASYNCQ_FULL";
        break;
    }
    case CITRUSLEAF_FAIL_TIMEOUT: {
        return "CITRUSLEAF_FAIL_TIMEOUT";
        break;
    }
    case CITRUSLEAF_FAIL_CLIENT: {
        return "CITRUSLEAF_FAIL_CLIENT";
        break;
    }
    case CITRUSLEAF_OK: {
        return "CITRUSLEAF_OK";
        break;
    }
    case CITRUSLEAF_FAIL_UNKNOWN: {
        return "CITRUSLEAF_FAIL_UNKNOWN";
        break;
    }
    case CITRUSLEAF_FAIL_NOTFOUND: {
        return "CITRUSLEAF_FAIL_NOTFOUND";
        break;
    }
    case CITRUSLEAF_FAIL_GENERATION: {
        return "CITRUSLEAF_FAIL_GENERATION";
        break;
    }
    case CITRUSLEAF_FAIL_PARAMETER: {
        return "CITRUSLEAF_FAIL_PARAMETER";
        break;
    }
    case CITRUSLEAF_FAIL_KEY_EXISTS: {
        return "CITRUSLEAF_FAIL_KEY_EXISTS";
        break;
    }
    case CITRUSLEAF_FAIL_BIN_EXISTS: {
        return "CITRUSLEAF_FAIL_BIN_EXISTS";
        break;
    }
    case CITRUSLEAF_FAIL_CLUSTER_KEY_MISMATCH: {
        return "CITRUSLEAF_FAIL_CLUSTER_KEY_MISMATCH";
        break;
    }
    case CITRUSLEAF_FAIL_PARTITION_OUT_OF_SPACE: {
        return "CITRUSLEAF_FAIL_PARTITION_OUT_OF_SPACE";
        break;
    }
    case CITRUSLEAF_FAIL_SERVERSIDE_TIMEOUT: {
        return "CITRUSLEAF_FAIL_SERVERSIDE_TIMEOUT";
        break;
    }
    case CITRUSLEAF_FAIL_NOXDS: {
        return "CITRUSLEAF_FAIL_NOXDS";
        break;
    }
    case CITRUSLEAF_FAIL_UNAVAILABLE: {
        return "CITRUSLEAF_FAIL_UNAVAILABLE";
        break;
    }
    case CITRUSLEAF_FAIL_INCOMPATIBLE_TYPE: {
        return "CITRUSLEAF_FAIL_INCOMPATIBLE_TYPE";
        break;
    }
    case CITRUSLEAF_FAIL_RECORD_TOO_BIG: {
        return "CITRUSLEAF_FAIL_RECORD_TOO_BIG";
        break;
    }
    case CITRUSLEAF_FAIL_KEY_BUSY: {
        return "CITRUSLEAF_FAIL_KEY_BUSY";
        break;
    }
    default:
          return "UNKNOWN ERROR";
    }
  } // end util_lookup_cl_errmsg()

/**
 * A Debug utility to print the contents of the Write Options structure.
 * This is the write info structure (defined in citrusleaf.h)
 * There's a lot of info that can go into a write ---
 * typedef struct {
 *  bool unique;  // write unique - success if didn't exist before
 *  bool unique_bin; // write unique bin:- success if the bin didn't exist 
 *  bool use_generation;// generation must be exact for write to succeed
 *  bool use_generation_gt;// generation must be less - good for backup & restore
 *  bool use_generation_dup;// on generation collision, create a duplicat
 *  uint32_t generation;
 *  int timeout_ms;
 *  uint32_t record_ttl;    // seconds, from now, when the record would be
 *                          // auto-removed from the DBcd 
 *  cl_write_policy w_pol;
 *  } cl_write_parameters;
 *
 */
void util_print_write_ops( cl_write_parameters *clwp ) {
  fprintf(stderr, "Citrusleaf Write Parameters::\n");
  fprintf(stderr, "unique(%s) \n", (clwp->unique ? "true":"false" ));
  fprintf(stderr, "unique_bin(%s) \n", (clwp->unique_bin ? "true":"false" ));
  fprintf(stderr, "use_generation(%s) \n",
    (clwp->use_generation ? "true":"false" ));
  fprintf(stderr, "use_generation_gt(%s) \n",
    (clwp->use_generation_gt ? "true":"false" ));
  fprintf(stderr, "use_generation_dup(%s) \n",
    (clwp->use_generation_dup ? "true":"false" ));
  fprintf(stderr, "generation(%d) \n", clwp->generation );
  fprintf(stderr, "timeout_ms(%d) \n", clwp->timeout_ms );
  fprintf(stderr, "record_ttl(%d) \n", clwp->record_ttl );
  fprintf(stderr, "cl_write_policy(%d) \n", clwp->w_pol );
}
  
/**
 * Construct a Return Tuple for the Erlang Function.  There are two
 * different return codes -- one is for the gateway function (GW RC),
 * which returns "error", and the other is for the Citrusleaf Client
 * function (CL RC), which returns "citrusleaf_error".  So, if nothing
 * really terrible happens in the GW function, it always returns GW_OK,
 * and any citrusleaf error code is passed along.
 */
static ERL_NIF_TERM
util_make_return_term_by_rc(ErlNifEnv* envp, GW_RC gw_rc, int cl_rc)
{
  static char * meth = "make_return_term_by_rc()";
  if( TRA_ENTER) fprintf(stderr, "%s[%s]: GW_RC(%d) CL_RC(%d)\n",
    D_ENTER, meth, gw_rc, cl_rc );

  if (GW_OK == gw_rc) {
    if (CITRUSLEAF_OK == cl_rc) {
      return enif_make_atom(envp, "ok");
    } else {
      if( TRA_DEBUG) fprintf(stderr, "%s[%s]: Make CL ERROR\n",D_DEBUG, meth );
      return enif_make_tuple2(envp,
              enif_make_atom(envp, "citrusleaf_error"),
              enif_make_tuple2(envp, enif_make_int(envp, cl_rc),
              enif_make_string(envp, util_lookup_cl_errmsg(cl_rc),
                ERL_NIF_LATIN1)));
    }
  } else {
    if( TRA_DEBUG) fprintf(stderr, "%s[%s]: Make CL ERROR\n",D_DEBUG, meth );
    return enif_make_tuple2(envp,
            enif_make_atom(envp, "error"),
            enif_make_string(envp, util_lookup_gw_errmsg(gw_rc),
              ERL_NIF_LATIN1));
  } // else return error tuple
} // end make_return_term_by_rc()

#if 0
/**
 * Call the Citrusleaf Gateway Init -- set up the server, etc.
 */
static ERL_NIF_TERM
clgwinit0_nif(ErlNifEnv* envp, int argc, const ERL_NIF_TERM argv[])
{
  int gw_rc = 0;
  static char * meth = "clgwinit0_nif()";

        if( TRA_ENTER)
                fprintf(stderr, "%s[%s]: argc(%d) \n", D_ENTER, meth, argc );

  // There are two paths to calling the gw_init (due to strange development
  // errors in the clgwinit function), so once everything is working we
  // might have the problem of trying to init twice.  Use our protection
  // against that.
  if( s_cl_init == 1 ){
    gw_rc = GW_OK;  // pretend like we did it.
  } else {
    gw_rc = gw_init();
    s_cl_init = 1; // now we've done it.
  }
        if (GW_OK == gw_rc) {
                return enif_make_atom(envp, "ok");
        } else {
                return enif_make_tuple2(envp,
                                        enif_make_atom(envp, "error"),
                                        enif_make_string(envp, util_lookup_gw_errmsg(gw_rc), ERL_NIF_LATIN1));
        } // else turn error tuple
} // end clgwinit0_nif()
#endif

/**
 * Connect to the Aerospike Server.  Unpack the Host and Port arguments
 * from the NIF object and make the Citrusleaf call.
 */
static ERL_NIF_TERM
connect2_nif(ErlNifEnv* envp, int argc, const ERL_NIF_TERM argv[])
{
  char host[HOST_NAME_MAX+5]; // HN_MX last seen at 255 (add padding)
  int port = 0;
  int con_h = 0;
  int len = 0;
  int rc = 0;
  static char * meth = "connect2_nif()";

  if( TRA_ENTER) fprintf(stderr, "%s[%s]: env(%p) argc(%d) \n",
    D_ENTER, meth, envp, argc );

  if( TRA_DEBUG) {
    fprintf(stderr, "%s[%s]: Argument Examination\n", D_DEBUG, meth );
    util_show_all_args( meth, envp, argc, argv );
  }

  // Step 0:  Call citrusleaf_init()
  // There are two paths to calling the gw_init (due to strange development
  // errors in the clgwinit function), so once everything is working we
  // might have the problem of trying to init twice.  Use our protection
  // against that.
  if( s_cl_init == 0 ){   // i.e.  If false, then ...
    if(( rc = gw_init()) < 0 ){
      if(TRA_ERROR) fprintf(stderr,"%s[%s]:CL Init Err(%d)\n", D_DEBUG,meth,rc);
    return util_make_error_tuple(envp, "Internal Error: CL init");
    }
    s_cl_init = 1; // now we've done it.
  } // otherwise, already done.

  // Step 1:  Get the Host Name
  if( TRA_DEBUG) fprintf(stderr, "%s[%s]: About to make string: NameMax(%d)\n",
    D_DEBUG, meth, HOST_NAME_MAX );
  len = enif_get_string(envp, argv[0], host, HOST_NAME_MAX, ERL_NIF_LATIN1);
  if (len > 0) {
    if( TRA_DEBUG)
      fprintf(stderr,"%s[%s]: OK: Str[%s](%d)\n", D_DEBUG, meth, host, len);
  } else {
    if( TRA_DEBUG)
      fprintf(stderr,"%s[%s]: String BAD: len(%d)\n", D_DEBUG, meth, len);
    return enif_make_badarg(envp);
  }

  if( TRA_DEBUG) fprintf(stderr,"%s[%s]: Check Name len(%d) Max(%d)\n",
    D_DEBUG, meth, len, HOST_NAME_MAX );
    // need to enforce the host name length since cl_client_gw has hard limit
  if (len > HOST_NAME_MAX) {
    if( TRA_ERROR) fprintf(stderr,"%s[%s]: Name(%s) len(%d) > Max(%d)\n",
      D_DEBUG, meth, host, len, HOST_NAME_MAX );
    return util_make_error_tuple(envp, "Hostname is too long");
  }

  // Step 2: Get the Port Name
  if( TRA_DEBUG) fprintf(stderr,"%s[%s]: Getting Port Val\n",D_DEBUG, meth);
    rc = enif_get_int(envp, argv[1], &port);
  if( TRA_DEBUG)
    fprintf(stderr,"%s[%s]:get Port results: rc(%d) port(%d)\n",
      D_DEBUG, meth, rc, port );
  if( rc == 0 ){
    if(TRA_DEBUG) fprintf(stderr,"%s[%s]:Bad Port argument\n",D_DEBUG,meth );
    return enif_make_badarg(envp);
  } else {
    if(TRA_DEBUG) fprintf(stderr,"%s[%s]:OK: Port(%d)\n",D_DEBUG,meth, port);
  }

  con_h = -1; // start pessimistic, keep hoping.
  int cl_rc;
  if(TRA_DEBUG) fprintf(stderr,"%s[%s]:Connect: Host(%s) Port(%d)\n",
    D_DEBUG, meth, host, port);
  if(TRA_DEBUG)
    fprintf(stderr,"%s[%s]:Calling gw_connect\n", D_DEBUG, meth );

  GW_RC gw_rc = gw_connect(host, port, &con_h, &cl_rc);
  if(TRA_DEBUG)
    fprintf(stderr,"%s[%s]:gw_connect: RC(%d)\n", D_DEBUG, meth, gw_rc);

  if ((GW_OK == gw_rc) && (con_h >= 0)) {     // all is good
    if( TRA_EXIT)
      fprintf(stderr, "%s[%s]: All Good: make return tuple.\n", D_EXIT, meth );
    return enif_make_tuple2(envp, enif_make_atom(envp, "ok"),
        enif_make_int(envp, con_h));
  } else {
    if( TRA_EXIT)
      fprintf(stderr, "%s[%s]: make ERROR return(%d)\n", D_EXIT, meth,gw_rc);
    return util_make_return_term_by_rc(envp, gw_rc, cl_rc);
  }
} // end connect2_nif()

/**
 * Add a host to the Aerospike Cluster.  Unpack the Erlang NIF Terms
 * and convert them into Citrusleaf C Structures, then make the 
 * Citrusleaf Client call.
 */
static ERL_NIF_TERM
addhost4_nif(ErlNifEnv* envp, int argc, const ERL_NIF_TERM argv[])
{
  int con_h;
  char host[HOST_NAME_MAX+5]; // HN_MX last seen at 255
  int port;
  unsigned timeout_ms;

  static char * meth = "addhost4_nif()";
  if(TRA_ENTER) fprintf(stderr,"%s[%s]: argc(%d)\n",D_ENTER,meth,argc);

  if( TRA_DEBUG) {
    fprintf(stderr, "%s[%s]: Argument Examination\n", D_DEBUG, meth );
    util_show_all_args( meth, envp, argc, argv );
  }

  // 1st parm is the connection handle
  if (!enif_get_int(envp, argv[0], &con_h)) {
    return enif_make_badarg(envp);
  }
  if (0 != validate_con_h(con_h)) {
    return util_make_error_tuple(envp, "Connection handle is not valid");
  }

  // 2nd parm is host name
  int len;
  len = enif_get_string(envp, argv[1], host, HOST_NAME_MAX+5, ERL_NIF_LATIN1);
  if (!len) {
          return enif_make_badarg(envp);
  }
  if (len > (HOST_NAME_MAX+1)) {
          return util_make_error_tuple(envp, "Hostname is too long");
  }

  // 3rd parm is port number
  if (!enif_get_int(envp, argv[2], &port)) {
          return enif_make_badarg(envp);
  }

  // 4th parm is timeout_ms
  if (!enif_get_uint(envp, argv[3], &timeout_ms)) {
          return enif_make_badarg(envp);
  }

  int cl_rc;
  int gw_rc = gw_addhost(con_h, host, port, timeout_ms, &cl_rc);
  return util_make_return_term_by_rc(envp, gw_rc, cl_rc);
} // end addhost4_nif()

/**
 * Name: shutdown1_nif
 * Synopsis: Shutdown a single connection
 * Error List and Structure:
 * (*) Single value (ERL_NIF_TERM: Unsigned int)
 * (1) Connection Handle Not Valid
 */
static ERL_NIF_TERM
shutdown1_nif(ErlNifEnv* envp, int argc, const ERL_NIF_TERM argv[])
{
  int con_h;
  static char * meth = "shutdown1_nif()";
  if(TRA_ENTER) fprintf(stderr,"%s[%s]: argc(%d)\n",D_ENTER,meth,argc);

  if( TRA_DEBUG) {
    fprintf(stderr, "%s[%s]: Argument Examination\n", D_DEBUG, meth );
    util_show_all_args( meth, envp, argc, argv );
  }

// 1st parm is the connection handle
  if (!enif_get_int(envp, argv[0], &con_h)) {
    return enif_make_badarg(envp);
  }
  if (0 != validate_con_h(con_h)) {
    return util_make_error_tuple(envp, "Connection handle is not valid");
  }

  GW_RC gw_rc = gw_shutdown(con_h);
  return util_make_return_term_by_rc(envp, gw_rc, CITRUSLEAF_OK);
} // end shutdown1_nif()

/**
 * Name: shutdownAll0_nif
 * Synopsis: Shutdown all connections
 * Error Structure:
 * - Single value (ERL_NIF_TERM: Unsigned int)
 * Errors:
 * - None (always returns ok).
 */
static ERL_NIF_TERM
shutdownAll0_nif(ErlNifEnv* envp, int argc, const ERL_NIF_TERM argv[])
{
  static char * meth = "shutdownAll0_nif()";
  if(TRA_ENTER) fprintf(stderr,"%s[%s]: argc(%d)\n",D_ENTER,meth,argc);

  GW_RC gw_rc = gw_shutdown_all();
  return util_make_return_term_by_rc(envp, gw_rc, CITRUSLEAF_OK);
} // end shutdownAll0_nif()

// Maximum number of properties: THis needs to move into the .h file
#define PROP_MAX 1024

/**
 * Make a call to the Citrusleaf Info method. Unpack the Erlang NIF Terms
 * and convert them into Citrusleaf C Structures, then make the 
 * Citrusleaf Client call.
 */
static ERL_NIF_TERM
clinfo2_nif(ErlNifEnv* envp, int argc, const ERL_NIF_TERM argv[])
{
  static char * meth = "clinfo2_nif()";
  if(TRA_ENTER) fprintf(stderr,"%s[%s]: argc(%d)\n",D_ENTER,meth,argc);

  ERL_NIF_TERM ret;
  int con_h;
  char *infovalues;
  char names[PROP_MAX];       // comma separated property names
  char *final_names;

  if( TRA_DEBUG) {
    fprintf(stderr, "%s[%s]: Argument Examination\n", D_DEBUG, meth );
    util_show_all_args( meth, envp, argc, argv );
  }

  // first parm is the connection handle
  if (!enif_get_int(envp, argv[0], &con_h)) {
    return enif_make_badarg(envp);
  }
  if (0 != validate_con_h(con_h)) {
    return util_make_error_tuple(envp, "Connection handle is not valid");
  }

  // second parm is the property name list
  // first try the search criteria as string, then try as an atom
  if (!enif_get_string(envp, argv[1], names, PROP_MAX, ERL_NIF_LATIN1)) {
    if (!enif_get_atom(envp, argv[0], names, PROP_MAX, ERL_NIF_LATIN1)) {
      return enif_make_badarg(envp);
    }
  }
  if (!strlen(names)) {
    final_names = NULL; // when no names, default is "all props"
  } else {
    final_names = names;
  }

  int cl_rc;
  if(TRA_DEBUG) fprintf(stderr,"%s[%s]: Calling gw_clinfo\n",D_ENTER,meth);
  GW_RC gw_rc = gw_clinfo(con_h, final_names, &infovalues, &cl_rc);
  if(TRA_DEBUG) fprintf(stderr,"%s[%s]: result(%d)\n",D_ENTER,meth,gw_rc);

  ERL_NIF_TERM term_result;
  if (infovalues) {
    term_result = ret = enif_make_string(envp, infovalues, ERL_NIF_LATIN1);
    free(infovalues);
  } else {
    term_result = util_make_return_term_by_rc(envp, gw_rc, cl_rc);
  }
  return term_result;
} // end clinfo2_nif()

// Caller must provide the dest obj. However, the needed data storage will
// be allocated.
#define OBJSZ_MAX 32768

// Convert an Erlang NIF term into a Citrusleaf Object.  Notice that we
// are stack allocating a buffer that will work for MOST calls, but we
// also check to see if our stack allocated buffer is large enough, and if
// it is not, we then allocate (malloc) the correct size and free it at the 
// end.
// RETURN:
// (*) > ZERO for ok (positive length)
// (*) <= ZERO for NOT OK:  Couldn't convert to a type.
static int
util_convert_term_to_cl_obj(ErlNifEnv* envp,
    const ERL_NIF_TERM term, cl_object *out_obj)
{
  // the term could be a bin value. 32K max. need to verify size limit
  char stack_buf[OBJSZ_MAX];
  char * malloc_buf = NULL; // Used only when we need MORE!!!
  char * buffptr; // Whichever one we use, this points to it.
  size_t buffsize = 0;
  bool release_memory = false;
  char *val;
  long long_value;
  int item_count = 0;
  int len = 0;  // Return the length of the converted object.
  int rc = 0;
  unsigned char * binary_buff;
  ErlNifBinary nif_binary;
  ERL_NIF_TERM binary_term;

  static char * meth = "util_convert_term_to_cl_obj()";
  if(TRA_ENTER) fprintf(stderr,"%s[%s]: \n",D_ENTER,meth );

  // Try to convert the value to various types -- and if we are successful
  // (meaning -- we get a non-zero length in return), then we know we have
  // the correct type.
  // We will try each type in turn and use the appropriate enif method
  // to access the value.
  //
  // If this is a BINARY term -- then create a temporary binary ENIF buffer.
  if (enif_is_binary(envp, term)){
    if(TRA_DEBUG) {
      fprintf(stderr,"%s[%s]: <<<BBBBBB>>>>  Term (%d) is binary!!!!\n",
      D_DEBUG, meth, term );
    }
    // We must look inside the TERM to see if our buffer is big enough.
    if(( rc = enif_inspect_binary( envp, term, &nif_binary )) < 0){
      if( TRA_ERROR )
        fprintf(stderr, "%s[%s]: Bad result from enif_inspect_binary(%d)\n",
            D_ERROR, meth, rc );
      return( 0 ); // no luck.  Ok to return, since we haven't allocated
                  // anything yet.
    }

    // Check and see if we need to allocate more memory.  If so, then
    // we do the malloc and remember it.  ALSO notice that we can't
    // RETURN -- we just GOTO Finish and check to see if there's memory
    // to release
    if( nif_binary.size > OBJSZ_MAX ){
      // Wow -- we need EVEN MORE MEMORY
      malloc_buf = malloc( nif_binary.size );
      buffptr = malloc_buf;
      buffsize = nif_binary.size;
      release_memory = true;
      out_obj->free = buffptr;
    } else {
      buffptr = stack_buf;  // static, don't free this one
      release_memory = false;
      buffsize = OBJSZ_MAX;
      out_obj->free = NULL;  // static, don't free this one
    }
    if( TRA_DETAIL ) {
      fprintf(stderr, "SHOW CONTENTS OF NIF BUFFER\n");
      util_print_bytes( nif_binary.data, nif_binary.size );
    }

    binary_buff = enif_make_new_binary( envp, nif_binary.size, &binary_term );
    if( TRA_DETAIL ) {
      fprintf(stderr, "SHOW CONTENTS OF CL BUFFER: After Make Binary\n");
      util_print_bytes( nif_binary.data, nif_binary.size );
    }
    memcpy( binary_buff, nif_binary.data, nif_binary.size );
    if( TRA_DETAIL ) {
      fprintf(stderr, "SHOW CONTENTS OF CL BUFFER: After Memcpy\n");
      util_print_bytes( nif_binary.data, nif_binary.size );
    }
    citrusleaf_object_init_blob( out_obj, binary_buff, nif_binary.size );
    len = nif_binary.size;
    if( TRA_EXIT ) fprintf(stderr, "%s[%s]: BINARY Len(%d)\n",
        D_EXIT, meth, len );
  } else if ((len =
    enif_get_atom(envp, term, stack_buf, OBJSZ_MAX, ERL_NIF_LATIN1)) > 0)
  {
    if(TRA_DEBUG) fprintf(stderr,"%s[%s]: Term: length(%d) is ATOM(%s)\n",
        D_DEBUG, meth, len, stack_buf );
    val = (char *)malloc(len);
    memcpy(val, stack_buf, len);
    citrusleaf_object_init_str(out_obj, val);
    out_obj->free = val;                            // set for cl obj free
  } else if ((len =
    enif_get_string(envp, term, stack_buf, OBJSZ_MAX, ERL_NIF_LATIN1)) > 0)
  {
    if(TRA_DEBUG) fprintf(stderr,"%s[%s]: Term:length(%d) is STRING(%s)\n",
        D_DEBUG, meth, len, stack_buf );
    val = (char *)malloc(len);
    memcpy(val, stack_buf, len);
    citrusleaf_object_init_str(out_obj, val);
    out_obj->free = val;                            // set for cl obj free
  } else if (( len = enif_get_long(envp, term, &long_value)) > 0 ){
    if(TRA_DEBUG) fprintf(stderr,"%s[%s]: Term:length(%d) is LONG(%d)\n",
        D_DEBUG, meth, len, long_value );
    citrusleaf_object_init_int(out_obj, long_value );
    // len = 1; // this seems wrong (tjl).
  } else if (( len = enif_is_list(envp, term )) > 0 ){
    if(TRA_DEBUG) fprintf(stderr,"%s[%s]: Term: RC(%d) is LIST\n",
        D_DEBUG, meth, len );
    rc =
      util_make_string_from_erlang_char_list( envp, term, stack_buf, OBJSZ_MAX);
    if( rc < 0 ){
      if(TRA_ERROR) fprintf(stderr,"%s[%s]: Fail: LIST to STRING: RC(%d)\n",
        D_ERROR, meth, rc );
      rc = 0;
      goto finish;  // don't return -- go to "cleanup" instead
    }
    val = (char *)malloc(len);
    memcpy(val, stack_buf, len);
    citrusleaf_object_init_str(out_obj, val);
    out_obj->free = val;     // set for cl obj free
  } else {
    // encountered enif type we still not supported yet
    if(TRA_DEBUG) {
      fprintf(stderr,"%s[%s]: Term (%d) is unsupported\n",
        D_DEBUG, meth, term );
      fprintf(stderr,"%s[%s]: !!!!!! SHOW THE TYPE THAT FAILED!!!!!\n",
        D_DEBUG, meth );
      util_return_as_type( envp, term );
    }
    len = 0;  // zero length return means .... BAD.
  } // end if-else chain

finish:
  if( release_memory )
    free( buffptr );

  return len;
} // end util_convert_term_to_cl_obj()


/**
 * util to extract the 4 lead arg parms that are common to many APIs
 */
static char*
util_extract_common_lead_parms(ErlNifEnv* envp, int argc,
    const ERL_NIF_TERM argv[], int *con_hp, char *ns, char *set,
    cl_object *key_objp)
{
  char stack_buf[NAMELEN_MAX];  // NL_MX last seen at 1024
  static char * meth = "util_extract_common_lead_parms()";
  if(TRA_ENTER) fprintf(stderr,"%s[%s]: argc(%d)\n",D_ENTER,meth,argc);


  // When we're debugging -- dump all of the types/values of the parms.
  if( TRA_DEBUG) {
    fprintf(stderr, "%s[%s]: Argument Examination:Conn,NS,Set,Key\n",
      D_DEBUG, meth );
    util_show_all_args( meth, envp, argc, argv );
  }

  // 1st parm is connection handle
  if (!enif_get_int(envp, argv[0], con_hp)) {
    fprintf(stderr, "%s[%s]:Connection Handle is bad C(%d)\n",
      D_DEBUG,meth,*con_hp );
    return "Connection handle is bad";
  }
  if (0 != validate_con_h(*con_hp)) {
    fprintf(stderr,
      "%s[%s]:Connection Handle Not Valid C(%d)\n", D_DEBUG, meth, *con_hp );
    return "Connection handle is not valid";
  }

  if( TRA_DEBUG ) {
    fprintf(stderr,
      "%s[%s]:Connection Handle OK C(%d): Next check NS\n",
        D_DEBUG, meth, *con_hp );
  }

  // 2nd parm is namespace
  // first try the search criteria as string, then try as an atom
  // note: enif_get_string and enif_get_atom will return byte count read,
  // including the null byte
  int len;
  len = enif_get_string(envp, argv[1], stack_buf, NAMELEN_MAX, ERL_NIF_LATIN1);
  if (!len) {
    len = enif_get_atom(envp, argv[1], stack_buf, NAMELEN_MAX, ERL_NIF_LATIN1);
    if (!len) {
      return "Namespace name must be a string or an atom";
    }
  }
  if( TRA_DEBUG ) {
    fprintf(stderr, "%s[%s]:NameSpace(%s) OK: Next check Set\n",
    D_DEBUG, meth, stack_buf );
  }

  // don't raise an error because of ns size limit. let the server decide
  strncpy(ns, stack_buf, NSNAME_MAX);

  // 3rd parm is the set
  // first try the search criteria as string, then try as an atom
  len = enif_get_string(envp, argv[2], stack_buf, NAMELEN_MAX, ERL_NIF_LATIN1);
  if (!len) {
    len = enif_get_atom(envp, argv[2], stack_buf, NAMELEN_MAX, ERL_NIF_LATIN1);
    if (!len) {
      return "Set name is bad";
    }
  }
  if( TRA_DEBUG ) {
    fprintf(stderr, "%s[%s]:Set(%s) OK: Next check Key\n",
      D_DEBUG, meth, stack_buf );
  }

  // don't raise an error because of set size limit. let the server decide
  strncpy(set, stack_buf, SETNAME_MAX);

  // 4rd parm is the key.
  // The value can be any datatype
  // Note: we do not check for key size limit. Let the server decide all
  // Jan 23, 2013, tjl:
  // New revelation:  Most keys that are constructed will arrive NOT as
  // a simple string, but instead as a LIST of terms.  That List of terms
  // may in fact be OTHER than character -- if the key was built using
  // dynamic operations (e.g. Erlang string concat() ).
  // So, we will have to carefully turn ALL terms into STRINGS or CHARS,
  // and then assemble them into a single string, which will THEN  be turned
  // into a Citrusleaf Key Object.  Notice that, at least for the key, this
  // is a one-way operation.  We don't have to convert it back.
  // Hopefully -- the user will typically "flatten" the strings, so that
  // no matter what they started with -- either a "string" type or a 
  // regular "list of chars" will land here.
  if(TRA_DEBUG) fprintf(stderr,
      "\nKKKKKKKKKKKKKKKKKKKK >> Convert Key <<KKKKKKKKKKKKKKKKKKKKK\n");
  
  len = util_convert_term_to_cl_obj(envp, argv[3], key_objp);
  if (len == 0) {
    if(TRA_ERROR)
      fprintf(stderr, "%s[%s] Couldn't figure out how to convert key object\n"
        D_ERROR, meth );
    return "Key is bad: Not of type atom, String or List";
  }
  if( TRA_DEBUG ) {
    fprintf(stderr, "%s[%s]:Key(%s) \n",
      D_DEBUG, meth, stack_buf );
  }

  return NULL;
} // end util_extract_common_lead_parms()

/**
 * Convert the Erlang Term (Tuple) to a Citrusleaf Record.
 * The tuple must have a very specific structure/contents, otherwise it is
 * an error:
 */
static int
util_convert_term_to_clwp_record(ErlNifEnv* envp,
    const ERL_NIF_TERM term, cl_write_parameters *clwp)
{
  int arity;
  const ERL_NIF_TERM *tuple;
  char str[32];
  int  nChange = 0;

  static char * meth = "util_convert_term_to_clwp_record()";
  if(TRA_ENTER) fprintf(stderr,"%s[%s]: \n",D_ENTER,meth );

  // a record must appear as a tuple
  if (!enif_get_tuple(envp, term, &arity, &tuple)) {
    if(TRA_DEBUG) fprintf(stderr,"%s[%s]: CLWOPT not a tuple\n",D_ENTER,meth );
    return 0;
  }

  // the record name, plus 9 fields
  if (arity != 10) {
    if(TRA_DEBUG) fprintf(stderr,"%s[%s]: CLWOPT Wrong 'arity' (%d) != 9\n",
        D_ENTER, meth, arity);
    return 0;
  }

  // Here's the structure of the Erlang Write Policy Tuple
  // -record(clwp, {unique, unique_bin, use_generation, use_generation_gt,
  // use_generation_dup, generation, timeout_ms, record_ttl, w_pol}).
  //
  int pos = 0;

  // record name = "clwp"
  enif_get_atom(envp, tuple[pos++], str, 50, ERL_NIF_LATIN1);
  if (0 != strcmp("clwp", str)) {
    if(TRA_DEBUG) fprintf(stderr,"%s[%s]: CLWOPT not 'clwp'\n",D_ENTER,meth );
    return 0;
  }

  // field = unique
  if (enif_get_atom(envp, tuple[pos++], str, 50, ERL_NIF_LATIN1)) {
    if(TRA_DEBUG) fprintf(stderr,"%s[%s]: Updating Unique(%s)\n",
        D_ENTER, meth, str );
    if (0 == strcmp("true", str))  { clwp->unique = true;  nChange++; }
    if (0 == strcmp("false", str)) { clwp->unique = false; nChange++; }
  }
  // field = unique_bin
  if (enif_get_atom(envp, tuple[pos++], str, 50, ERL_NIF_LATIN1)) {
    if(TRA_DEBUG) fprintf(stderr,"%s[%s]: Updating UniqueBin(%s)\n",
        D_ENTER, meth, str );
    if (0 == strcmp("true", str))  { clwp->unique_bin = true;  nChange++; }
    if (0 == strcmp("false", str)) { clwp->unique_bin = false; nChange++; }
  }
  // field = use_generation
  if (enif_get_atom(envp, tuple[pos++], str, 50, ERL_NIF_LATIN1)) {
    if (0 == strcmp("true", str))  { clwp->use_generation = true;  nChange++; }
    if (0 == strcmp("false", str)) { clwp->use_generation = false; nChange++; }
  }
  // field = use_generation_gt
  if (enif_get_atom(envp, tuple[pos++], str, 50, ERL_NIF_LATIN1)) {
    if (0 == strcmp("true", str))
      { clwp->use_generation_gt = true;  nChange++; }
    if (0 == strcmp("false", str))
      { clwp->use_generation_gt = false; nChange++; }
  }
  // field = use_generation_dup
  if (enif_get_atom(envp, tuple[pos++], str, 50, ERL_NIF_LATIN1)) {
    if (0 == strcmp("true", str))
      { clwp->use_generation_dup = true;  nChange++; }
    if (0 == strcmp("false", str))
    { clwp->use_generation_dup = false; nChange++; }
  }
  // field = generation
  int  ui;
  if (enif_get_int(envp, tuple[pos++], &ui)) {
          clwp->generation = ui; nChange++;
  }
  // field = timeout_ms
  int  i;
  if (enif_get_int(envp, tuple[pos++], &i)) {
          clwp->timeout_ms = i; nChange++;
  }
  // field = record_ttl
  if (enif_get_int(envp, tuple[pos++], &ui)) {
          clwp->record_ttl = ui; nChange++;
  }
  // field = w_pol
  if (enif_get_atom(envp, tuple[pos++], str, 50, ERL_NIF_LATIN1)) {
    if (0 == strcmp("cl_write_async", str))
      { clwp->w_pol = CL_WRITE_ASYNC;  nChange++; }
    if (0 == strcmp("cl_write_oneshot", str))
      { clwp->w_pol = CL_WRITE_ONESHOT;  nChange++; }
    if (0 == strcmp("cl_write_retry", str))
      { clwp->w_pol = CL_WRITE_RETRY;  nChange++; }
    if (0 == strcmp("cl_write_assured", str))
      { clwp->w_pol = CL_WRITE_ASSURED;  nChange++; }
  }

  if( TRA_DEBUG ){
    // Write the Citrusleaf Write Parameters Object -- if we're debugging.
    util_print_write_ops( clwp );
  }

  // Return the number of changes so that the caller(s) will know if they
  // should send the CLWP object -- or null (if no changes).
  return nChange;
} // end util_convert_term_to_clwp_record()

/**
 * Process the Citrusleaf PUT Call:  Convert each of the parameters from
 * Erlang (NIF) into CL and make the call.
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
static ERL_NIF_TERM
put6_nif(ErlNifEnv* envp, int argc, const ERL_NIF_TERM argv[])
{
  int con_h;
  char ns[NSNAME_MAX+1];
  char set[SETNAME_MAX+1];
  cl_object key_obj;

  ns[NSNAME_MAX] = '\0';
  set[SETNAME_MAX] = '\0';

  static char * meth = "put6_nif()";
  if(TRA_ENTER) fprintf(stderr,"%s[%s]: argc(%d)\n",D_ENTER,meth,argc);

  if( TRA_DEBUG) {
    fprintf(stderr, "%s[%s]: Argument Examination\n", D_DEBUG, meth );
    util_show_all_args( meth, envp, argc, argv );
  }

  // this takes care of the first 4 parms
  char* parm_error_msg =
    util_extract_common_lead_parms(envp, argc, argv, &con_h, ns, set, &key_obj);
  if (parm_error_msg) {
    return util_make_error_tuple(envp, parm_error_msg);
  }

  if( TRA_DEBUG) {
    fprintf(stderr, "%s[%s]: Have processed the first FOUR parms.\n",
        D_DEBUG, meth );
    fprintf(stderr, "%s[%s]: Now Processing the TUPLE for BINS/VALUES.\n",
        D_DEBUG, meth );
  }
  // 5th parm is a list of: [{bin, val}, ...]
  unsigned tuple_count;
  ERL_NIF_TERM head;
  ERL_NIF_TERM tail;
  if (!enif_get_list_length(envp, argv[4], &tuple_count)) {
    if( TRA_ERROR) {
      fprintf(stderr, "%s[%s]: Problems Processing Bin/Values Tuple.(1)\n",
        D_ERROR, meth );
    }
    citrusleaf_object_free(&key_obj);
    return util_make_error_tuple(envp,
      "Bin-Val list must be in [{bin1, val1}, {bin2, val2} ...] format");
  }

  if (!tuple_count) {
    if( TRA_ERROR) {
      fprintf(stderr, "%s[%s]: Problems Processing Bin/Values Tuple.(2)\n",
        D_ERROR, meth );
    }
    citrusleaf_object_free(&key_obj);
    return util_make_error_tuple(envp, "{Biname,Value} list cannot be empty");
  }

  // we don't limit the number of bins at the client side.
  // let the server decide on that
  if (!enif_get_list_cell(envp, argv[4], &head, &tail)) {
    return util_make_error_tuple(envp, "Failed to get Binname-Value list");
  }

  cl_bin *my_bins;
  my_bins = (cl_bin *)malloc(tuple_count*sizeof(cl_bin));
  if (!my_bins) {
    return util_make_error_tuple(envp, "out of memory");
  }
  unsigned i;
  for (i = 0; i< tuple_count; i++) {
    citrusleaf_object_init(&my_bins[i].object);
  }

  // the bin-value list appears this way:  [{bin1, val1}, {bin2, val2} ... ]
  for (i = 0; i < tuple_count; i++) {
    const ERL_NIF_TERM *tuple;
    int arity;
    enif_get_tuple(envp, head, &arity, &tuple);
    if (arity != 2) {
      if( TRA_ERROR)
        fprintf(stderr, "%s[%s]: B/V Tuple: Wrong 'arity'(%d)\n",
        D_ERROR, meth, arity);
      citrusleaf_object_free(&key_obj);
      citrusleaf_bins_free(my_bins, tuple_count);
      free(my_bins);
      return util_make_error_tuple(envp,
      "Binname-Value list must be in [{bin1, val1}, {bin2, val2} ...] format");
    } // end if

    // binname can be string or atom; try the cell as a string,
    // then try again as an atom
    char binname[BINNAME_MAX+5];
    int len;
    len = enif_get_string(envp,tuple[0],binname, BINNAME_MAX+5, ERL_NIF_LATIN1);
    if (!len) {
      len = enif_get_atom(envp,tuple[0],binname, BINNAME_MAX+5, ERL_NIF_LATIN1);
      if (!len) {
        citrusleaf_bins_free(my_bins, tuple_count);
        free(my_bins);                          // only one free for all bins
        return util_make_error_tuple(envp,
          "Bin name must be a string or an atom");
      } // end if
    }
    if (len > BINNAME_MAX) {
      citrusleaf_bins_free(my_bins, tuple_count);
      free(my_bins);
      return util_make_error_tuple(envp,
        "Binname exceeded 32 characters client limit");
    }
    if( TRA_DEBUG) {
      fprintf(stderr, "%s[%s]: Now Processing VALUE for bin(%d)\n",
      D_DEBUG, meth, i );
    }
    strcpy(my_bins[i].bin_name, binname);
    util_convert_term_to_cl_obj(envp, tuple[1], &my_bins[i].object);

    // move on to the next list member
    enif_get_list_cell(envp, tail, &head, &tail);
  }

  // the 6th parm is clwopts. It's record, so here appears as a tuple,
  // if present
  cl_write_parameters clwp;
  cl_write_parameters *clwp_ptr;

  if (enif_is_tuple(envp, argv[5])) {
    if( TRA_DEBUG ) fprintf(stderr, "PUT: Converting CLWOPTS Tuple\n");
    cl_write_parameters_set_default(&clwp);
    int nChange = util_convert_term_to_clwp_record(envp, argv[5], &clwp);
    // printf("nChange2 = %d\n", nChange);
    if (nChange) {
      // found at least one set value
      if( TRA_DEBUG ) fprintf(stderr, "PUT: Found a CLWOPTS Option\n");
      clwp_ptr = &clwp;
    } else {
      // use default if not set values found
      clwp_ptr = NULL;
      if( TRA_DEBUG ) fprintf(stderr, "PUT: NULL CLWOPTS \n");
    }
  } else {
    // not set, use default
    clwp_ptr = NULL;
    if( TRA_DEBUG ) fprintf(stderr, "PUT: NULL CLWOPTS \n");
  }

  int cl_rc;
  GW_RC gw_rc =
    gw_put(con_h, ns, set, &key_obj, my_bins, tuple_count, clwp_ptr, &cl_rc);

  if( TRA_EXIT ) fprintf(stderr, "%s[%s]: Exit Put Results:GWRC(%d) CLRC(%d)\n",
      D_EXIT, meth, gw_rc, cl_rc );

  citrusleaf_bins_free(my_bins, tuple_count);
  return util_make_return_term_by_rc(envp, gw_rc, cl_rc);
} // end put6_nif()

/**
 *
 */
static ERL_NIF_TERM
util_convert_cl_bin_to_term(ErlNifEnv* envp, cl_bin *bins, int n_bins)
{
  int i;
  static char * meth = "util_convert_cl_bin_to_term()";
  if(TRA_ENTER) fprintf(stderr,"%s[%s]: NBins(%d)\n", D_ENTER, meth, n_bins);

  ERL_NIF_TERM curr_head = 0;
  curr_head = enif_make_list(envp, 0);
  for (i = 0; i < n_bins; i++) { // For EACH Bin ...
    ERL_NIF_TERM binname_term; // enif TERM for the CL Bin
    ERL_NIF_TERM binval_term;  // enif TERM for the CL Value

    binname_term = enif_make_string(envp, bins[i].bin_name, ERL_NIF_LATIN1);
    if( TRA_DEBUG ){
      fprintf(stderr, "%s[%s]: Term(%d) of (%d) Type(%d)[%s]\n",
          D_DEBUG, meth, i, n_bins, bins[i].object.type,
          util_show_type_name( bins[i].object.type ));
    }

    switch (bins[i].object.type) {
    case CL_STR: {
      binval_term =  enif_make_string(envp, bins[i].object.u.str, ERL_NIF_LATIN1);
      break;
    }
    case CL_INT:
    case CL_TIMESTAMP: {
      binval_term =  enif_make_long(envp, bins[i].object.u.i64);
      break;
    }
    case CL_NULL: {
      binval_term =  enif_make_atom(envp, "null");
      break;
    }
    case CL_FLOAT: {
      // we should not see float, not yet
      binval_term =
        enif_make_string(envp, "(warning: CL_FLOAT encounted)", ERL_NIF_LATIN1);
      break;
    }
    case CL_BLOB:
    case CL_JAVA_BLOB:
    case CL_CSHARP_BLOB:
    case CL_PYTHON_BLOB:
    case CL_DIGEST:
    case CL_PHP_BLOB:
    case CL_ERLANG_BLOB: // Woo Hoo!!
    case CL_RUBY_BLOB: {
      if(TRA_DEBUG) fprintf(stderr,"!!!!! Processing BLOB !!!! \n");

      ErlNifBinary out_binary;
      if (!enif_alloc_binary(bins[i].object.sz, &out_binary)) {
        binval_term =  enif_make_atom(envp, "alloc_binary_failed");
        if(TRA_DEBUG)
          fprintf(stderr,"%s[%s}: MAKING BINARY ATOM\n", D_DEBUG, meth);
      } else {
        if (0 == bins[i].object.sz) {
          binval_term =  enif_make_binary(envp, &out_binary);
        if(TRA_DEBUG)
          fprintf(stderr,"%s[%s}: MAKING BINARY: Sz ZERO(1)\n", D_DEBUG, meth);
        } else {
        if(TRA_DEBUG)
          fprintf(stderr,"%s[%s}: MAKING BINARY (2)\n", D_DEBUG, meth);
        if( TRA_DETAIL )
        util_print_bytes( bins[i].object.u.blob, bins[i].object.sz );

          memcpy(out_binary.data, bins[i].object.u.blob, bins[i].object.sz);
          binval_term = enif_make_binary(envp, &out_binary);
          // no need to release out_binary; enif_make_binary takes
          // ownership of the memory
        } // end else
      } // end else
      break;
    } // end case
    case CL_UNKNOWN:
    default: {
      binval_term =
      enif_make_string(envp,"(unknown data type encountered)", ERL_NIF_LATIN1);
      break;
    } // end default
    } // end switch()

    curr_head = enif_make_list_cell(envp,
                 enif_make_tuple2(envp, binname_term, binval_term), curr_head);
  } // end for each BIN

  ERL_NIF_TERM result;
  enif_make_reverse_list(envp, curr_head, &result);
  return result;
} // end util_convert_cl_bin_to_term()

// Convert the Erlang NIF Terms to Citusleaf BINs
// if there is any error, then create an error message (a description)
// to the caller.  If no error message, then we return a NULL pointer.
// The Caller checks for NULL to see if everything is good.
static char*
util_convert_termlist_to_receiving_cl_bins(ErlNifEnv *envp, ERL_NIF_TERM term,
    cl_bin **bins, int *n_bins)
{
  int isList = 1;
  unsigned my_n;
  cl_bin *my_bins;

  static char * meth = "util_convert_termlist_to_receiving_cl_bins()";
  if(TRA_ENTER) fprintf(stderr,"%s[%s]: \n", D_ENTER, meth );

  // determine how many bins we'll  need
  if (!enif_is_list(envp, term)) {
    // not a list
    isList = 0;
    my_n = 1;     // fake a list with one cell
  } else {
    if (!enif_get_list_length(envp, term, &my_n)) {
      // must have list size to continue
      return "List of bin names is malformed";
    }
  }

  my_bins =  (cl_bin *)malloc(my_n * sizeof(cl_bin));
  if (!my_bins) {
    return "Out of memory";
  }
  unsigned i;
  for (i = 0; i< my_n; i++) {
    citrusleaf_object_init(&my_bins[i].object);
  }


  ERL_NIF_TERM head;
  ERL_NIF_TERM tail;
  if (isList) {
    if (!enif_get_list_cell(envp, term, &head, &tail)) {
      return "Bin name list is malformed";
    }
  } else {
    head = term;
  }

  // We need to be able to handle list of names, or just a single name
  for (i = 0; i < my_n; i++) {
    // binname can be string or atom; try the cell as a string,
    // then try again as an atom
    char binname[BINNAME_MAX+5];
    int len;
    len = enif_get_string(envp, head, binname, BINNAME_MAX+5, ERL_NIF_LATIN1);
    if (!len) {
      len = enif_get_atom(envp, head, binname, BINNAME_MAX+5, ERL_NIF_LATIN1);
      if (!len) {
        citrusleaf_bins_free(my_bins, my_n);
        free(my_bins);                          // only one free for all bins
        return "Bin name is malformed";
      }
    }
    if (strlen(binname) > BINNAME_MAX) {
      citrusleaf_bins_free(my_bins, my_n);
      free(my_bins);
      return "Binname exceeded 32 characters limit";
    }
    strncpy(my_bins[i].bin_name, binname, BINNAME_MAX);
    citrusleaf_object_init( &my_bins[i].object );    // get ready for read

    // move to the next list member
    if (isList) {
      enif_get_list_cell(envp, tail, &head, &tail);
    }
  }

  *bins = my_bins;
  *n_bins = my_n;

  // all is well. No error desc
  return NULL;
} // end util_convert_termlist_to_receiving_cl_bins()

/**
 * Do the Erlang NIF to Citrusleaf Parameter translation and then call
 * the actual Citrusleaf_get() function.  Perform the reverse translation
 * of the answers (CL to NIF).
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
static ERL_NIF_TERM
get6_nif(ErlNifEnv* envp, int argc, const ERL_NIF_TERM argv[])
{
  int con_h;
  char ns[NSNAME_MAX+1];
  char set[SETNAME_MAX+1];
  cl_object key;
  unsigned timeout_ms;

  static char * meth = "get6_nif()";
  if(TRA_ENTER) fprintf(stderr,"%s[%s]: argc(%d)\n",D_ENTER,meth,argc);

  if( TRA_DETAIL ) {
    fprintf(stderr, "%s[%s]: Argument Examination\n", D_DEBUG, meth );
    util_show_all_args( meth, envp, argc, argv );
  }

  // this takes care of the first 4 parms
  char* parm_error_msg =
    util_extract_common_lead_parms(envp, argc, argv, &con_h, ns, set, &key);
  if (parm_error_msg) {
    return util_make_error_tuple(envp, parm_error_msg);
  }

  // 5th parm is the list of bin names
  cl_bin *bins;
  int n_bins;
  parm_error_msg =
    util_convert_termlist_to_receiving_cl_bins(envp, argv[4], &bins, &n_bins);
  if (parm_error_msg) {
    citrusleaf_object_free(&key);
    return util_make_error_tuple(envp, parm_error_msg);
  }

  // 6th parm is timeout_ms
  if (!enif_get_uint(envp, argv[5], &timeout_ms)) {
    citrusleaf_object_free(&key);
    return enif_make_badarg(envp);
  }

  int cl_rc;
  GW_RC gw_rc = gw_get(con_h, ns, set, &key, bins, n_bins, timeout_ms, &cl_rc);

  ERL_NIF_TERM term_result;
  if ((GW_OK == gw_rc) && (CITRUSLEAF_OK == cl_rc)) {
    // All good -- create an Erlang Results Tuple
    term_result = util_convert_cl_bin_to_term(envp, bins, n_bins);
  } else {
    // Not so good -- create an error response tuple
    term_result = util_make_return_term_by_rc(envp, gw_rc, cl_rc);
  }

  citrusleaf_object_free(&key);
  citrusleaf_bins_free(bins, n_bins); // free all memory allocated in each bin
  free(bins);                         // only one free for all bins

  return term_result;
} // end get6_nif()

/**
 * Do the Erlang NIF to Citrusleaf Parameter translation and then call
 * the actual Citrusleaf_getall() function.  Perform the reverse translation
 * of the answers (CL to NIF).
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
static ERL_NIF_TERM
getall5_nif(ErlNifEnv* envp, int argc, const ERL_NIF_TERM argv[])
{
  int con_h;
  char ns[NSNAME_MAX+1];
  char set[SETNAME_MAX+1];
  cl_object key_obj;
  unsigned timeout_ms;

  ns[NSNAME_MAX] = '\0';
  set[SETNAME_MAX] = '\0';

  static char * meth = "getall5()";
  if(TRA_ENTER) fprintf(stderr,"%s[%s]: argc(%d)\n",D_ENTER,meth,argc);

  // this takes care of the first 4 parms
  if( TRA_DEBUG) {
    fprintf(stderr, "%s[%s]: Argument Examination\n", D_DEBUG, meth );
    util_show_all_args( meth, envp, argc, argv );
  }

  char* parm_error_msg =
    util_extract_common_lead_parms(envp, argc, argv, &con_h, ns, set, &key_obj);
  if (parm_error_msg) {
    return util_make_error_tuple(envp, parm_error_msg);
  }

  // 5th parm is timeout_ms
  if (!enif_get_uint(envp, argv[4], &timeout_ms)) {
    return util_make_error_tuple(envp, "Timeout value is bad");
  }

  ERL_NIF_TERM term_result;
  cl_bin *bins = 0;
  int n_bins;
  int cl_rc;
  GW_RC gw_rc =
    gw_getall(con_h, ns, set, &key_obj, &bins, &n_bins, timeout_ms, &cl_rc);

  if ((GW_OK == gw_rc) && (CITRUSLEAF_OK == cl_rc)) {
    if(TRA_DEBUG) fprintf(stderr, "%s[%s]: Get returns OK\n", D_DEBUG, meth);
    // All good -- create the Results Tuple
    term_result = util_convert_cl_bin_to_term(envp, bins, n_bins);
  } else {
    if(TRA_DEBUG) fprintf(stderr, "%s[%s]: Get Bad: GW RC(%d) CL RC(%d)\n",
        D_DEBUG, meth, gw_rc, cl_rc );
    // Not good -- create the Error Response Tuple
    term_result = util_make_return_term_by_rc(envp, gw_rc, cl_rc);
  }

  citrusleaf_object_free(&key_obj);
  citrusleaf_bins_free(bins, n_bins);
  free(bins);                             // only one free for all bins

  return term_result;
} // end getall5()

/**
 * Delete: Unpack the parameters and call gw_delete()
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
static ERL_NIF_TERM
delete5_nif(ErlNifEnv* envp, int argc, const ERL_NIF_TERM argv[])
{
  int con_h;
  char ns[NSNAME_MAX+1];
  char set[SETNAME_MAX+1];
  cl_object key_obj;
  unsigned timeout_ms;

  ns[NSNAME_MAX] = '\0';
  set[SETNAME_MAX] = '\0';

  static char * meth = "delete5_nif()";
  if(TRA_ENTER) fprintf(stderr,"%s[%s]: argc(%d)\n",D_ENTER,meth,argc);

  if( TRA_DEBUG) {
    fprintf(stderr, "%s[%s]: Argument Examination\n", D_DEBUG, meth );
    util_show_all_args( meth, envp, argc, argv );
  }

  // this takes care of the first 4 parms
  if(TRA_DEBUG)
    fprintf(stderr, "%s[%s]: Calling Extract Parms\n", D_DEBUG, meth );
  char* parm_error_msg =
  util_extract_common_lead_parms(envp, argc, argv, &con_h, ns, set, &key_obj);
  if (parm_error_msg) {
    return util_make_error_tuple(envp, parm_error_msg);
  }
  if(TRA_DEBUG) fprintf(stderr,"%s[%s]:Parms: ConH(%d) ns(%s) set(%s) \n",
    D_DEBUG, meth, con_h, ns, set );

  // the 5th parm is clwopts. It's record, so here appears as a tuple,
  // if present
  cl_write_parameters clwp;
  cl_write_parameters *clwp_ptr;

  if (enif_is_tuple(envp, argv[4])) {
    if(TRA_DEBUG) fprintf(stderr,"%s[%s]:Converting\n",D_DEBUG,meth);
    cl_write_parameters_set_default(&clwp);
    int nChange = util_convert_term_to_clwp_record(envp, argv[5], &clwp);
    // printf("nChange2 = %d\n", nChange);
    if(TRA_DEBUG) fprintf(stderr,"%s[%s]:nChange2(%d)\n",D_DEBUG,meth,nChange);
    if (nChange) {
      // found at least one set value
      clwp_ptr = &clwp;
    } else {
      // use default if not set values found
      clwp_ptr = NULL;
    }
  } else {
    if( TRA_DEBUG) fprintf(stderr,"%s[%s]: not get\n",D_DEBUG,meth );
    // not set, use default
    clwp_ptr = NULL;
  }

  int cl_rc;
  if(TRA_DEBUG) fprintf(stderr,"%s[%s]: Calling gw_delete\n", D_DEBUG, meth);
  GW_RC gw_rc = gw_delete(con_h, ns, set, &key_obj, clwp_ptr, &cl_rc);
  if(TRA_DEBUG) fprintf(stderr,"%s[%s]:gw_delete(RC=%d)\n",D_DEBUG,meth,gw_rc);

  return util_make_return_term_by_rc(envp, gw_rc, cl_rc);
} // end delete5_nif()

/**
 * Perform the Histogram function -- and return the results in NIF
 * TERM Format.
 */
static ERL_NIF_TERM
histogram1_nif(ErlNifEnv* envp, int argc, const ERL_NIF_TERM argv[])
{
  char command[10];
  GW_RC gw_rc;

  time_t lapsed;
  char r_report[1024];
  char w_report[1024];

  static char * meth = "histogram1_nif()";
  if(TRA_ENTER) fprintf(stderr,"%s[%s]: argc(%d)\n",D_ENTER,meth,argc);

  if( TRA_DEBUG) {
    fprintf(stderr, "%s[%s]: Argument Examination\n", D_DEBUG, meth );
  util_show_all_args( meth, envp, argc, argv );
}


  if (!enif_get_atom(envp, argv[0], command, 10, ERL_NIF_LATIN1)) {
    return enif_make_badarg(envp);
  }

  // call the appropriate gw function
  if (0 == strcmp(command, "start")) {
    gw_rc = gw_histogram_start();
    return util_make_return_term_by_rc(envp, gw_rc, CITRUSLEAF_OK);
  } else if (0 == strcmp(command, "report")) {
    gw_rc = gw_histogram_report(&lapsed, w_report, 1024, r_report, 1024);
  } else {
    return enif_make_badarg(envp);
  }

  // if there are errors, no need to present the histograms
  if (GW_OK != gw_rc) {
    return util_make_return_term_by_rc(envp, gw_rc, CITRUSLEAF_OK);
  }

  ERL_NIF_TERM curr_head = 0;
  curr_head = enif_make_list(envp, 0);
  curr_head = enif_make_list_cell(envp,
      enif_make_tuple2(envp, enif_make_atom(envp, "seconds"),
        enif_make_int(envp, lapsed)), curr_head);

  curr_head = enif_make_list_cell(envp,
                enif_make_tuple2(envp, enif_make_atom(envp, "writehistogram"),
                  enif_make_string(envp, w_report, ERL_NIF_LATIN1)), curr_head);
  curr_head = enif_make_list_cell(envp,
      enif_make_tuple2(envp,
        enif_make_atom(envp, "readhistogram"),
        enif_make_string(envp, r_report, ERL_NIF_LATIN1)), curr_head);
  return curr_head;
} // end histogram1_nif()

/**
 * Perform the stopwatch function -- and return the stopwatch report
 * in Erlang NIF Terms.
 */
static ERL_NIF_TERM
stopwatch1_nif(ErlNifEnv* envp, int argc, const ERL_NIF_TERM argv[])
{
  char command[10];
  GW_RC gw_rc;

  time_t lapsed;
  int nwrite;
  int nread;

  static char * meth = "stopwatch1_nif()";
  if(TRA_ENTER) fprintf(stderr,"%s[%s]: argc(%d)\n",D_ENTER,meth,argc);

  if( TRA_DEBUG) {
    fprintf(stderr, "%s[%s]: Argument Examination\n", D_DEBUG, meth );
    util_show_all_args( meth, envp, argc, argv );
  }

  if (!enif_get_atom(envp, argv[0], command, 10, ERL_NIF_LATIN1)) {
    return enif_make_badarg(envp);
  }

  // call the appropriate gw function
  if (0 == strcmp(command, "start")) {
    gw_rc = gw_stopwatch_start();
    return util_make_return_term_by_rc(envp, gw_rc, CITRUSLEAF_OK);
  }
  else if (0 == strcmp(command, "stop")) {
    gw_rc = gw_stopwatch_stop(&lapsed, &nwrite, &nread);
  }
  else if (0 == strcmp(command, "report")) {
    gw_rc = gw_stopwatch_report(&lapsed, &nwrite, &nread);
  }
  else {
    return enif_make_badarg(envp);
  }

  // if there are errors, no need to present the numeric results
  if (GW_OK != gw_rc) {
    return util_make_return_term_by_rc(envp, gw_rc, CITRUSLEAF_OK);
  }

  int write_tps = nwrite/lapsed;
  int read_tps = nread/lapsed;

  ERL_NIF_TERM curr_head = 0;
  curr_head = enif_make_list(envp, 0);
  curr_head = enif_make_list_cell(envp,
      enif_make_tuple2(envp, enif_make_atom(envp, "seconds"),
                              enif_make_int(envp, lapsed)), curr_head);
  curr_head = enif_make_list_cell(envp,
        enif_make_tuple2(envp, enif_make_atom(envp, "writes"),
        enif_make_int(envp, nwrite)), curr_head);
  curr_head = enif_make_list_cell(envp,
        enif_make_tuple2(envp,
        enif_make_atom(envp, "reads"), enif_make_int(envp, nread)),
      curr_head);
  curr_head = enif_make_list_cell(envp,
      enif_make_tuple2(envp,
        enif_make_atom(envp, "writeTPS"),
        enif_make_int(envp, write_tps)),
      curr_head);
  curr_head = enif_make_list_cell(envp,
      enif_make_tuple2(envp,
        enif_make_atom(envp, "readTPS"),
        enif_make_int(envp, read_tps)),
      curr_head);
  return curr_head;
} // end stopwatch1_nif()


// Show the set of NIF functions that are defined in this module.
// They are pulled into the NIF environment by the INIT() call below.
// Usage:
// {"erl client name", Num Args, C client function name}
// Note:  For reasons I still don't understand, we get some sort of weird
// load error when we define "clgwinit".  However, this call is not needed
// because the actual function is performed during connect().
static ErlNifFunc nif_funcs[] = {
        {"addhost",     4, addhost4_nif},
        // {"clgwinit",     0, clgwinit0_nif},
        {"clinfo",      2, clinfo2_nif},
        {"connect",     2, connect2_nif},
        {"delete",      5, delete5_nif},
        {"get",         6, get6_nif},
        {"getAll",      5, getall5_nif},
        {"histogram",   1, histogram1_nif},
        {"put",         6, put6_nif},
        {"shutdown",    1, shutdown1_nif},
        {"stopwatch",   1, stopwatch1_nif},
        {"shutdownAll", 0, shutdownAll0_nif}
};

//
// IMPORTANT: !!!
// This is the magic macro to initialize the NIF library.
// ERL_NIF_INIT(MODULE< ErlNifFunc funcs[], load, reload, upgrade, unload)
//
// It must be evaluated in the global file scope.
// >MODULE< (aerospike) is the name of the Erlang Module - an identifier
// without string quotations.  It will be "stringified" by the Macro.
// >funcs< is a static array of function descriptors for all of the
// implemented NIFs in this library.
// >??< as far as I know -- we are not using the remaining functions.
// Useful Reading: www.erlang.org/doc/man/erl_nif.html
// Jan 2013, tjl
ERL_NIF_INIT(aerospike, nif_funcs, NULL, NULL, NULL, NULL);

// <<<<<<<<<<<<<<<<<<<<<<<<<<< END OF FILE >>>>>>>>>>>>>>>>>>>>>>>>>>>
