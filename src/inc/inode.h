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
#ifndef __INODE_H__
#define __INODE_H__

#include <stddef.h>

#include <pthread.h>
#include <misc/hlist.h>
#include <types.h>

#include <sys/stat.h>


#define MYFS_TYPE_DEL	(1ul << 0)
#define MYFS_TYPE_REG	S_IFREG
#define MYFS_TYPE_DIR	S_IFDIR

#define MYFS_INODE_NEW	(1ul << 0)


struct __myfs_bmap_entry {
	le64_t disk_offs;
	le64_t file_offs;
} __attribute__((packed));

struct myfs_bmap_entry {
	uint64_t disk_offs;
	uint64_t file_offs;
};

struct __myfs_inode_key {
	le64_t inode;
} __attribute__((packed));

struct __myfs_bmap {
	le32_t size;
	struct __myfs_bmap_entry entry[1];
} __attribute__((packed));

struct __myfs_inode_value {
	le64_t size;
	le64_t mtime;
	le64_t ctime;
	le32_t links;
	le32_t type;
	le32_t uid;
	le32_t gid;
	le32_t perm;
	struct __myfs_bmap bmap;
} __attribute__((packed));


struct myfs_bmap {
	uint32_t size;
	struct myfs_bmap_entry *entry;
};

struct myfs_inode {
	struct hlist_node ll;
	pthread_rwlock_t rwlock;
	unsigned long flags;
	unsigned long refcnt;
	uint64_t inode;
	uint64_t size;
	uint64_t mtime;
	uint64_t ctime;
	uint32_t links;
	uint32_t type;
	uint32_t uid;
	uint32_t gid;
	uint32_t perm;
	struct myfs_bmap bmap;
};

struct myfs_icache {
	struct hlist_head *head;
	pthread_mutex_t *mtx;
	uint64_t a, b;
	size_t bits;
};


void __myfs_icache_setup(struct myfs_icache *cache, size_t bits);
void myfs_icache_setup(struct myfs_icache *cache);
void myfs_icache_release(struct myfs_icache *cache);


struct myfs_inode *myfs_inode_get(struct myfs *myfs, uint64_t ino);
void __myfs_inode_put(struct myfs *myfs, struct myfs_inode *inode,
			unsigned long refcnt);
void myfs_inode_put(struct myfs *myfs, struct myfs_inode *inode);

int __myfs_inode_write(struct myfs *myfs, struct myfs_inode *inode);
int __myfs_inode_read(struct myfs *myfs, struct myfs_inode *inode);
int myfs_inode_read(struct myfs *myfs, struct myfs_inode *inode);


struct myfs_lsm_sb;
struct myfs_lsm;

void myfs_inode_map_setup(struct myfs_lsm *lsm, struct myfs *myfs,
			const struct myfs_lsm_sb *sb);
void myfs_inode_map_release(struct myfs_lsm *lsm);

#endif /*__INODE_H__*/
