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
#ifndef __WRITE_AHEAD_LOG_H__
#define __WRITE_AHEAD_LOG_H__

#include <types.h>
#include <myfs.h>
#include <misc/list.h>

#include <pthread.h>


#define MYFS_WAL_NONE	0
#define MYFS_WAL_ENTRY	1
#define MYFS_WAL_JUMP	2


struct __myfs_wal_entry {
	le32_t size;
	le32_t csum;
	le32_t type;
} __attribute__((packed));

struct __myfs_wal_ptr {
	le64_t offs;
	le32_t size;
} __attribute__((packed));


struct myfs_wal_buf {
	pthread_mutex_t mtx;
	uint64_t offs;

	void *buf;
	size_t size;
	size_t cap;
};

struct myfs_wal {
	struct myfs *myfs;
	int err;

	struct myfs_wal_buf buf[2];
	struct myfs_wal_buf *current;
	struct myfs_wal_buf *next;

	struct list_head wait_current;
	struct list_head wait_next;
	pthread_spinlock_t lock;
};


void myfs_wal_setup(struct myfs_wal *wal, struct myfs *myfs);
void myfs_wal_release(struct myfs_wal *wal);


struct myfs_trans {
	struct list_head ll;

	void *buf;
	size_t cap, size;

	struct __myfs_wal_entry *entry;
	int wait;
	pthread_mutex_t mtx;
	pthread_cond_t cv;
};


void myfs_wal_trans_setup(struct myfs_trans *trans);
void myfs_wal_trans_release(struct myfs_trans *trans);
void myfs_wal_trans_append(struct myfs_trans *trans,
			const void *data, size_t size);
void myfs_wal_trans_finish(struct myfs_trans *trans);

int myfs_wal_append(struct myfs_wal *wal, struct myfs_trans *trans);

#endif /*__WRITE_AHEAD_LOG_H__*/
