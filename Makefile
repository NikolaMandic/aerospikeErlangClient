# include Makefile.files
# NOTE: Aerospike changed its name from Citrusleaf to Aerospike in Sept 2012,
# and, as a result, both names are used in both client and server terminology.
# Although the new name is "Aerospike", the old name, "Citrusleaf" still
# appears in many places.  Over time, the number of Citrusleaf occurrences
# will diminish.
#
# For Compiling the Aerospike Module and Aerospike Native Implemented Functions
# we must link to the correct (current) Erlang library that is installed
# on YOUR machine.  We do that by setting an environment variable (EIVER).
#
# Set EIVER according to /usr/local/lib/erlang/lib/erl_interface-(n.n.n)
# Do this:   ls /usr/local/lib/erlang/lib/erl_interface*
# Set this EIVER variable to match the erl_interface-n.n.n numbers
# I used to have have R15B01 ==> erl_interface-3.7.7
# EIVER = 3.7.7
#
# However, I currently have R16A ==> erl_interface-3.7.10
EIVER = 3.7.17

# Erlang Include
DIR_ERL = /usr/lib/erlang
ERL_INCL = $(DIR_ERL)/usr/include

# These directories are all local from this Makefile's position
# The Local Code (Erlang Client) Include
CLIENT_INCL = ./include

# Aerospike Citrusleaf Include
# The source files already expect to use "citrusleaf/citrusleaf.h", so do NOT
# add the extra directory here.
C_INCL = ../c/include

# Aerospike Client  Source and Objects
DIR_SRC = ./src
C_DIR_OBJ = ../c/obj/native
C_DIR_SRC = ../c/src
ER_DIR_OBJ = ./obj
ER_DIR_SRC = ./src
ER_ERL_DIR = .

# Set the VPATH to look in all these places for things
VPATH = $(C_DIR_OJB) $(ER_DIR_OJB) 
VPATH += $(C_DIR_SRC) $(ER_DIR_SRC) 
VPATH += .

DIR_TARGET = .

C_SOURCE = cf_alloc.c cf_average.c cf_digest.c cf_hist.c cf_hooks.c
C_SOURCE += cf_ll.c cf_log.c
C_SOURCE += cf_queue.c cf_service.c cf_shash.c cf_socket.c cf_vector.c 
C_SOURCE += citrusleaf.c cl_async.c cl_batch.c cl_cluster.c cl_info.c
C_SOURCE += cl_lookup.c cl_partition.c cl_request.c cl_scan.c cf_proto.c
C_SOURCE += cl_shm.c version.c

ER_SOURCE = aerospike_nif.c  cl_client_gw.c

C_OBJECTS = $(C_SOURCE:%.c=$(C_DIR_OBJ)/%.o)
ER_OBJECTS = $(ER_SOURCE:%.c=$(ER_DIR_OBJ)/%.o)

# The main AEROSPIKE.ERL file (what all apps need to include) will be
# in the main Erlang directory -- along with the actual Erlang apps
# themselves -- unless users want to change this make file to adjust to
# other Erlang source/object directories.
ERL_SRC = aerospike.erl 
ERL_BEAM_OBJECTS = $(ERL_SRC:%.erl=%.beam)

ERL_EXAMPLE_SRC = asbench.erl stats_record.erl
ERL_BEAM_EXAMPLE_OBJECTS = $(ERL_EXAMPLE_SRC:%.erl=%.beam)

OBJECTS = $(C_OBJECTS) $(ER_OBJECTS) 

INCLUDES = -I$(ERL_INCL) -I$(CLIENT_INCL) -I$(C_INCL)

CC = gcc
LD = $(CC)
# Needed to make everything available to the NIF ERLANG module
SONAME = aerospike_nif.so

TARGET_D = $(SONAME)
#-shared 
LDFLAGS = -shared -Wl,-soname=$(SONAME) 
LDFLAGS += -L$(DIR_ERL)/lib/erl_interface-$(EIVER)/lib -L. -L/usr/lib/
LDLIBS =  -lerl_interface -pthread -lssl -pthread -lrt -lcitrusleaf -lcrypto

AS_CFLAGS = -D_FILE_OFFSET_BITS=64 -std=gnu99 -D_REENTRANT
MARCH_NATIVE = $(shell uname -m)
# ----------------------------------------------------------
# Pick the right level of compile -- debug or optimize
# Debug Compile
CFLAGS_NATIVE = -g -fno-common -fno-strict-aliasing -rdynamic  -Wextra $(AS_CFLAGS) -D MARCH_$(MARCH_NATIVE)
# Optimized Compile
# CFLAGS_NATIVE = -g -O3  -fno-common -fno-strict-aliasing -rdynamic  -Wextra $(AS_CFLAGS) -D MARCH_$(MARCH_NATIVE)
# ----------------------------------------------------------

all: $(TARGET_D) $(ERL_BEAM_OBJECTS) $(ERL_BEAM_EXAMPLE_OBJECTS)

examples: $(ERL_BEAM_EXAMPLE_OBJECTS) $(ERL_BEAM_OBJECTS)

ER_OBJECTS : $(HEADERS) $(SOURCES)

$(ER_DIR_OBJ)/%.o: $(ER_DIR_SRC)/%.c
	$(CC)  $(CFLAGS_NATIVE) -fPIC -shared $(INCLUDES) -o $@ -c $<

$(TARGET_D): $(ER_OBJECTS)
	  $(LD) $(LDFLAGS) -o $(TARGET_D) $(OBJECTS) $(LDLIBS) 

# The Beams and Erls are all in the current dir, and the Beams depend
# on the Erls.
# ERL_BEAM_OBJECTS: $(ERL_SRC)
# /usr/local/bin/erlc $<
aerospike.beam: aerospike.erl aerospike.hrl
	/usr/bin/erlc aerospike.erl

asbench.beam: asbench.erl aerospike.beam aerospike.hrl
	/usr/bin/erlc asbench.erl

stats_record.beam: stats_record.erl aerospike.beam aerospike.hrl
	/usr/bin/erlc stats_record.erl

clean: 
	/bin/rm -rf $(ER_DIR_OBJ)/*
	/bin/rm -rf $(SONAME)
	/bin/rm -rf $(ERL_BEAM_OBJECTS)
	/bin/rm -rf $(ERL_BEAM_EXAMPLE_OBJECTS)
# ------------------------------------------
#
