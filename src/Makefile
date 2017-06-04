CC ?= gcc
AR ?= ar

FUSE_CFLAGS := $(shell pkg-config fuse3 --cflags)
CFLAGS := -g -Wall -Wextra -Werror -pedantic -std=c11 \
	-Wframe-larger-than=4096 -Wstack-usage=4096 \
	-D_XOPEN_SOURCE=500 -D_GNU_SOURCE \
	-Wno-unknown-warning-option $(if $(DEBUG),-DDEBUG -Og,-O3)
FUSE_LFLAGS := $(shell pkg-config fuse3 --libs)
LFLAGS := -pthread

INC := ./inc
SRC := ./src
TST := ./test
UTL := ./utils

C_SOURCES := $(shell find $(SRC) -name *.c)
C_OBJECTS := $(C_SOURCES:.c=.o)

TST_SOURCES := $(shell find $(TST) -name *.c)
TSTS := $(TST_SOURCES:.c=)

DEP := $(shell find ./ -name '*.d')
OBJ := $(C_OBJECTS)

default: libmyfs.a $(TSTS) myfs-mkfs myfs-fuse

$(TSTS): %: %.c libmyfs.a
	$(CC) -I$(INC) $(CFLAGS) -g -MMD $< -L. -lmyfs $(LFLAGS) -o $@

myfs-mkfs: utils/mkfs.c libmyfs.a
	$(CC) -I$(INC) $(CFLAGS) -g -MMD $< -L. -lmyfs $(LFLAGS) -o $@

myfs-fuse: utils/fuse.c libmyfs.a
	$(CC) -I$(INC) $(CFLAGS) $(FUSE_CFLAGS) -g -MMD $< -L. -lmyfs $(LFLAGS) $(FUSE_LFLAGS) -o $@

libmyfs.a: $(OBJ)
	$(AR) rcs $@ $(OBJ)

$(C_OBJECTS): %.o: %.c
	$(CC) $(CFLAGS) -I$(INC) -g -MMD -c $< -o $@

-include $(DEP)

.PHONY: clean
clean:
	rm -f libmyfs.a $(TSTS) $(OBJ) $(DEP) myfs-mkfs myfs-fuse
