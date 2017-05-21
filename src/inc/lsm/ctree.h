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
#ifndef __CTREE_H__
#define __CTREE_H__

#include <types.h>

#include <stddef.h>


#define MYFS_MAX_CTREE_HIGHT	8
#define MYFS_MIN_FANOUT		16


struct __myfs_ctree_item {
	le32_t key_size;
	le32_t value_size;
} __attribute__((packed));

struct myfs_ctree_item {
	uint32_t key_size;
	uint32_t value_size;
};


static inline void myfs_ctree_item2disk(struct __myfs_ctree_item *disk,
			const struct myfs_ctree_item *mem)
{
	disk->key_size = htole32(mem->key_size);
	disk->value_size = htole32(mem->value_size);
}

static inline void myfs_ctree_item2mem(struct myfs_ctree_item *mem,
			const struct __myfs_ctree_item *disk)
{
	mem->key_size = le32toh(disk->key_size);
	mem->value_size = le32toh(disk->value_size);
}


struct __myfs_ctree_node_sb {
	le32_t items;
	le32_t size;
} __attribute__((packed));

struct myfs_ctree_node_sb {
	uint32_t items;
	uint32_t size;
};


static inline void myfs_ctree_node_sb2disk(struct __myfs_ctree_node_sb *disk,
			const struct myfs_ctree_node_sb *mem)
{
	disk->items = htole32(mem->items);
	disk->size = htole32(mem->size);
}

static inline void myfs_ctree_node_sb2mem(struct myfs_ctree_node_sb *mem,
			const struct __myfs_ctree_node_sb *disk)
{
	mem->items = le32toh(disk->items);
	mem->size = le32toh(disk->size);
}


struct __myfs_ctree_sb {
	struct __myfs_ptr root;
	le32_t size;
	le32_t hight;
} __attribute__((packed));

struct myfs_ctree_sb {
	struct myfs_ptr root;
	uint64_t size;
	uint32_t hight;
};

static inline void myfs_ctree_sb2disk(struct __myfs_ctree_sb *disk,
			const struct myfs_ctree_sb *mem)
{
	myfs_ptr2disk(&disk->root, &mem->root);
	disk->hight = htole32(mem->hight);
	disk->size = htole64(mem->size);
}

static inline void myfs_ctree_sb2mem(struct myfs_ctree_sb *mem,
			const struct __myfs_ctree_sb *disk)
{
	myfs_ptr2mem(&mem->root, &disk->root);
	mem->hight = le32toh(disk->hight);
	mem->size = le64toh(disk->size);
}


struct myfs_ctree_node {
	struct myfs_ptr ptr;
	struct myfs_ctree_node_sb sb;

	size_t buf_sz;
	void *buf;

	struct myfs_key *key;
	struct myfs_value *value;
};

struct myfs_ctree_it {
	struct myfs_ctree_sb sb;
	struct myfs_ctree_node node[MYFS_MAX_CTREE_HIGHT];
	size_t pos[MYFS_MAX_CTREE_HIGHT];

	struct myfs_key key;
	struct myfs_value value;
};


void myfs_ctree_it_setup(struct myfs_ctree_it *it,
			const struct myfs_ctree_sb *sb);
void myfs_ctree_it_release(struct myfs_ctree_it *it);
int myfs_ctree_it_reset(struct myfs *myfs, struct myfs_ctree_it *it);
int myfs_ctree_it_find(struct myfs *myfs, struct myfs_ctree_it *it,
			struct myfs_query *query);
int myfs_ctree_it_next(struct myfs *myfs, struct myfs_ctree_it *it);
int myfs_ctree_it_valid(const struct myfs_ctree_it *it);


struct myfs_query;

int myfs_ctree_lookup(struct myfs *myfs, const struct myfs_ctree_sb *sb,
			struct myfs_query *query); 
int myfs_ctree_range(struct myfs *myfs, const struct myfs_ctree_sb *sb,
			struct myfs_query *query);


struct myfs_ctree_buffer {
	size_t size;

	size_t buf_offs;
	size_t buf_size;

	size_t key_offs;
	size_t key_size;

	size_t value_offs;
	size_t value_size;
};

struct myfs_ctree_level {
	struct myfs_ctree_buffer *node;
	size_t size;
	size_t cap;

	size_t buf_size;
	size_t buf_cap;
	void *buf;
};

struct myfs_ctree_builder {
	struct myfs_ctree_sb sb;
	struct myfs_ctree_level level[MYFS_MAX_CTREE_HIGHT + 1];
};


void myfs_builder_setup(struct myfs_ctree_builder *builder);
void myfs_builder_release(struct myfs_ctree_builder *builder);

int myfs_builder_append(struct myfs *myfs, struct myfs_ctree_builder *builder,
			const struct myfs_key *key,
			const struct myfs_value *value);
int myfs_builder_finish(struct myfs *myfs, struct myfs_ctree_builder *builder);

#endif /*__CTREE_H__*/
