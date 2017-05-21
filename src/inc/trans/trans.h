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
#ifndef __TRANS_H__
#define __TRANS_H__

#include <types.h>


#define MYFS_TRANS_REPLAYED	(1 << 0)


struct __myfs_log_item {
	le32_t type;
	le32_t key_size;
	le32_t value_size;
} __attribute__((packed));

struct myfs_log_item {
	unsigned type;
	size_t key_size;
	size_t value_size;
};


static inline void myfs_log_item2disk(struct __myfs_log_item *disk,
			const struct myfs_log_item *mem)
{
	disk->type = htole32(mem->type);
	disk->key_size = htole32(mem->key_size);
	disk->value_size = htole32(mem->value_size);
}

static inline void myfs_log_item2mem(struct myfs_log_item *mem,
			const struct __myfs_log_item *disk)
{
	mem->type = le32toh(disk->type);
	mem->key_size = le32toh(disk->key_size);
	mem->value_size = le32toh(disk->value_size);
}


struct __myfs_slot_sb {
	le32_t items;
} __attribute__((packed));

struct myfs_slot_sb {
	size_t items;
};


static inline void myfs_slot_sb2disk(struct __myfs_slot_sb *disk,
			const struct myfs_slot_sb *mem)
{
	disk->items = htole32(mem->items);
}

static inline void myfs_slot_sb2mem(struct myfs_slot_sb *mem,
			const struct __myfs_slot_sb *disk)
{
	mem->items = le32toh(disk->items);
}


struct __myfs_trans_sb {
	le64_t trans_id;
	le64_t slots;
} __attribute__((packed));

struct myfs_trans_sb {
	uint64_t trans_id;
	uint64_t slots;
};


static inline void myfs_trans_sb2disk(struct __myfs_trans_sb *disk,
			const struct myfs_trans_sb *mem)
{
	disk->trans_id = htole64(mem->trans_id);
	disk->slots = htole64(mem->slots);
}

static inline void myfs_trans_sb2mem(struct myfs_trans_sb *mem,
			const struct __myfs_trans_sb *disk)
{
	mem->trans_id = le64toh(disk->trans_id);
	mem->slots = le64toh(disk->slots);
}


struct myfs_slot {
	struct myfs_ptr ptr;
	uint64_t flags;
};

struct myfs_trans {
	struct myfs_ptr ptr;
	uint64_t trans_id;
	struct myfs_slot *slot;
	size_t size;
};


struct myfs_trans_scanner {
	int (*emit)(struct myfs_trans_scanner *, unsigned,
				const struct myfs_key *,
				const struct myfs_value *);
};


void myfs_trans_setup(struct myfs_trans *trans);
void myfs_trans_release(struct myfs_trans *trans);

int myfs_trans_parse(struct myfs *myfs, struct myfs_trans *trans,
			const struct myfs_ptr *ptr);
int myfs_trans_scan(struct myfs *myfs, const struct myfs_trans *trans,
			struct myfs_trans_scanner *scanner);


struct myfs_log {
	/**
	 * only valid after myfs_log_register, used to mark log
	 * as replayed inside the transaction pointed by trans_id.
	 **/
	uint64_t trans_id;
	size_t offs;

	size_t size, cap;
	struct myfs_ptr *ptr;

	size_t buf_sz, buf_cap, buf_entries;
	void *data;
};


void myfs_log_setup(struct myfs_log *log);
void myfs_log_release(struct myfs_log *log);


int myfs_log_append(struct myfs *myfs, struct myfs_log *log,
			unsigned type,
			const struct myfs_key *key,
			const struct myfs_value *value);
int myfs_log_finish(struct myfs *myfs, struct myfs_log *log);
int myfs_log_scan(struct myfs *myfs, struct myfs_log *log,
			struct myfs_trans_scanner *scanner);


void myfs_log_register(struct myfs_trans *trans, struct myfs_log *log);
void myfs_log_commit(struct myfs_trans *trans, struct myfs_log *log);

#endif /*__TRANS_H__*/
