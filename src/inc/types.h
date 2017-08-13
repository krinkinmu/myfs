/*
   Copyright 2017, Mike Krinkin <krinkin.m.u@gmail.com>

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#ifndef __MYFS_TYPES_H__
#define __MYFS_TYPES_H__

#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>

#include <endian.h>


#define MYFS_FS_MAGIC	0x13131313ul
#define MYFS_FS_ROOT	1

#ifndef htole8
#define htole8(x)	((uint8_t)(x))
#endif

typedef uint64_t le64_t;
typedef uint32_t le32_t;
typedef uint16_t le16_t;
typedef uint8_t le8_t;


struct myfs_key {
	size_t size;
	void *data;
};

struct myfs_value {
	size_t size;
	void *data;
};


static inline void myfs_entry_release(struct myfs_key *key,
			struct myfs_value *value)
{
	free(key->data); key->data = NULL; key->size = 0;
	free(value->data); value->data = NULL; value->size = 0;
}


struct myfs_query {
	int (*cmp)(struct myfs_query *, const struct myfs_key *);
	int (*emit)(struct myfs_query *, const struct myfs_key *,
				const struct myfs_value *);
};


struct __myfs_ptr {
	/* all disks offsets and sizes are always given in file system
	   page sizes */
	le64_t offs;
	le64_t csum;
	le16_t size;
} __attribute__((packed));

struct myfs_ptr {
	uint64_t offs;
	uint64_t csum;
	uint16_t size;
};


static inline void myfs_ptr2disk(struct __myfs_ptr *disk,
			const struct myfs_ptr *mem)
{
	disk->offs = htole64(mem->offs);
	disk->csum = htole64(mem->csum);
	disk->size = htole16(mem->size);
}

static inline void myfs_ptr2mem(struct myfs_ptr *mem,
			const struct __myfs_ptr *disk)
{
	mem->offs = le64toh(disk->offs);
	mem->csum = le64toh(disk->csum);
	mem->size = le16toh(disk->size);
}


typedef int (*myfs_cmp_t)(const struct myfs_key *, const struct myfs_key *);
typedef int (*myfs_del_t)(const struct myfs_key *, const struct myfs_value *);
struct myfs;


#endif /*__MYFS_TYPES_H__*/
