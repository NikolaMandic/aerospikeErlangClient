# Citrusleaf Tools
# Makefile

DEPTH = ../../..
include Makefile.in

DIR_INCLUDE = ../include $(DEPTH)/include
DIR_OBJECT = ../obj
DIR_TARGET = ../bin

HEADERS = loop.h shash.h
SOURCES = main.c loop.c shash.c
TARGET = loopTest

INCLUDES = $(DIR_INCLUDE:%=-I%) 
LDFLAGS += -L$(DEPTH)/lib
LIBRARIES = -lcitrusleaf  -lssl -lcrypto -lpthread -lrt -lz

OBJECTS = $(SOURCES:%.c=$(DIR_OBJECT)/%.o)
DEPENDENCIES = $(OBJECTS:%.o=%.d)

.PHONY: all
all: loopTest

.PHONY: clean
clean:
	/bin/rm -f $(OBJECTS) $(DIR_TARGET)/$(TARGET)

.PHONY: depclean
depclean: clean
	/bin/rm -f $(DEPENDENCIES)

.PHONY: loopTest
loopTest: $(OBJECTS)
	$(CC) $(LDFLAGS) -o $(DIR_TARGET)/$(TARGET) $(OBJECTS) $(LIBRARIES)

-include $(DEPENDENCIES)

$(DIR_OBJECT)/%.o: %.c
	$(CC) $(CFLAGS_NATIVE) -MMD -o $@ -c $(INCLUDES) $<
