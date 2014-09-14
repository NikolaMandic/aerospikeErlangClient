# Citrusleaf Tools
# Makefile

DEPTH = ../../..
include Makefile.in

DIR_INCLUDE = ../include $(DEPTH)/include
DIR_OBJECT = ../obj
DIR_TARGET = ../bin

HEADERS = key.h 
SOURCES = main.c key.c histogram.c
TARGET = keyTest

#
# note: pthread for threads and locks
# RT for good clock functions
# ssl for random number generator
#
INCLUDES = $(DIR_INCLUDE:%=-I%) 
LDFLAGS += -L$(DEPTH)/lib
LIBRARIES = -lcitrusleaf -lssl -lcrypto -lpthread -lrt -lz

OBJECTS = $(SOURCES:%.c=$(DIR_OBJECT)/%.o)
DEPENDENCIES = $(OBJECTS:%.o=%.d)

.PHONY: all
all: keyTest

.PHONY: clean
clean:
	/bin/rm -f $(OBJECTS) $(DIR_TARGET)/$(TARGET)

.PHONY: depclean
depclean: clean
	/bin/rm -f $(DEPENDENCIES)

.PHONY: keyTest
keyTest: $(OBJECTS)
	$(CC) $(LDFLAGS) -g -o $(DIR_TARGET)/$(TARGET) $(OBJECTS) $(LIBRARIES)

-include $(DEPENDENCIES)

$(DIR_OBJECT)/%.o: %.c
	$(CC) $(CFLAGS_NATIVE) -MMD -o $@ -c $(INCLUDES) $<
