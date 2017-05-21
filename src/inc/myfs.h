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
#ifndef __MYFS_H__
#define __MYFS_H__

#include <stdatomic.h>
#include <stdint.h>
#include <stddef.h>
#include <assert.h>
#include <string.h>

#include <endian.h>

#include <lsm/lsm.h>
#include <inode.h>
#include <types.h>


#define MYFS_FS_MAGIC	0x13131313ul
#define MYFS_FS_ROOT	1
#define MYFS_FS_NAMEMAX	256


struct __myfs_check {
	le64_t csum;
	le64_t gen;
	le64_t ino;
	struct __myfs_lsm_sb inode_sb;
	struct __myfs_lsm_sb dentry_sb;
} __attribute__((packed));

struct myfs_check {
	uint64_t csum;
	uint64_t gen;
	uint64_t ino;
	struct myfs_lsm_sb inode_sb;
	struct myfs_lsm_sb dentry_sb;
};


static inline void myfs_check2disk(struct __myfs_check *disk,
			const struct myfs_check *mem)
{
	disk->csum = 0;
	disk->ino = htole64(mem->ino);
	disk->gen = htole64(mem->gen);
	myfs_lsm_sb2disk(&disk->inode_sb, &mem->inode_sb);
	myfs_lsm_sb2disk(&disk->dentry_sb, &mem->dentry_sb);
}

static inline void myfs_check2mem(struct myfs_check *mem,
			const struct __myfs_check *disk)
{
	mem->csum = le64toh(disk->csum);
	mem->ino = le64toh(disk->ino);
	mem->gen = le64toh(disk->gen);
	myfs_lsm_sb2mem(&mem->inode_sb, &disk->inode_sb);
	myfs_lsm_sb2mem(&mem->dentry_sb, &disk->dentry_sb);
}


struct __myfs_sb {
	le32_t magic;
	le32_t page_size;
	le32_t check_size;
	le64_t check_offs;
	le64_t backup_check_offs;
	le64_t root;
} __attribute__((packed));

struct myfs_sb {
	uint32_t magic;
	uint32_t page_size;
	uint32_t check_size;
	uint64_t check_offs;
	uint64_t backup_check_offs;
	uint64_t root;
};


static inline void myfs_sb2disk(struct __myfs_sb *disk,
			const struct myfs_sb *mem)
{
	disk->magic = htole32(mem->magic);
	disk->page_size = htole32(mem->page_size);
	disk->check_size = htole32(mem->check_size);
	disk->check_offs = htole64(mem->check_offs);
	disk->backup_check_offs = htole64(mem->backup_check_offs);
	disk->root = htole64(mem->root);
}

static inline void myfs_sb2mem(struct myfs_sb *mem,
			const struct __myfs_sb *disk)
{
	mem->magic = le32toh(disk->magic);
	mem->page_size = le32toh(disk->page_size);
	mem->check_size = le32toh(disk->check_size);
	mem->check_offs = le64toh(disk->check_offs);
	mem->backup_check_offs = le64toh(disk->backup_check_offs);
	mem->root = le64toh(disk->root);
}


struct myfs {
	struct myfs_sb sb;
	struct myfs_check check;

	struct bdev *bdev;

	struct myfs_inode *root;

	struct myfs_lsm dentry_map;
	struct myfs_lsm inode_map;
	struct myfs_icache icache;

	uint64_t page_size;
	size_t fanout;
	atomic_uint_least64_t next_ino;

	/* disk space allocator stub - position of the next free page */
	atomic_uint_least64_t next_offs;

	/* for simple lock based not durable transactions, before we
	   implement propper WAL. */
	pthread_rwlock_t trans_lock;
	pthread_t flusher;
	atomic_int done;

	/* to prevent concurrent commits */
	pthread_mutex_t commit_lock;
};


/* page size is always power of two. */
static inline uint64_t myfs_align_down(uint64_t x, uint64_t page_size)
{
	assert(!(page_size & (page_size - 1)));
	return x & ~(page_size - 1);
}

static inline uint64_t myfs_align_up(uint64_t x, uint64_t page_size)
{
	assert(!(page_size & (page_size - 1)));
	return myfs_align_down(x + page_size - 1, page_size);
}

static inline uint64_t myfs_timespec2stamp(const struct timespec *spec)
{
	uint64_t stamp = spec->tv_sec;

	stamp *= 1000;
	stamp += spec->tv_nsec / 1000000;
	return stamp;
}

static inline void myfs_stamp2timespec(struct timespec *spec, uint64_t stamp)
{
	spec->tv_sec = stamp / 1000;
	spec->tv_nsec = (long)1000000 * (stamp % 1000);
}

uint64_t myfs_now(void);

uint64_t myfs_csum(const void *buf, size_t size);
uint32_t myfs_hash(const void *buf, size_t size);
int myfs_mount(struct myfs *myfs, struct bdev *bdev);
void myfs_unmount(struct myfs *myfs);
int myfs_checkpoint(struct myfs *myfs);
int myfs_commit(struct myfs *myfs);


static inline void myfs_trans_start(struct myfs *myfs)
{
	assert(!pthread_rwlock_rdlock(&myfs->trans_lock));
}

static inline void myfs_trans_finish(struct myfs *myfs)
{
	assert(!pthread_rwlock_unlock(&myfs->trans_lock));
}


int myfs_lookup(struct myfs *myfs, struct myfs_inode *dir, const char *name,
			struct myfs_inode **inode);
int myfs_create(struct myfs *myfs, struct myfs_inode *dir, const char *name,
			uid_t uid, gid_t gid, mode_t mode,
			struct myfs_inode **inode);
int myfs_unlink(struct myfs *myfs, struct myfs_inode *dir, const char *name);
int myfs_rmdir(struct myfs *myfs, struct myfs_inode *dir, const char *name);
int myfs_rename(struct myfs *myfs, struct myfs_inode *old, const char *oldname,
			struct myfs_inode *new, const char *newname);
int myfs_link(struct myfs *myfs, struct myfs_inode *inode,
			struct myfs_inode *dir, const char *newname);

struct myfs_dentry;
struct myfs_readdir_ctx {
	int (*emit)(struct myfs_readdir_ctx *ctx, const struct myfs_dentry *);
};

int myfs_readdir(struct myfs *myfs, struct myfs_inode *inode,
			struct myfs_readdir_ctx *ctx, uint64_t cookie);
long myfs_read(struct myfs *myfs, struct myfs_inode *inode,
			void *data, size_t size, off_t off);
long myfs_write(struct myfs *myfs, struct myfs_inode *inode,
			const void *data, size_t size, off_t off);

int myfs_block_write(struct myfs *myfs, const void *buf, uint64_t size,
			uint64_t offs);
int myfs_block_read(struct myfs *myfs, void *buf, uint64_t size, uint64_t offs);
int myfs_block_sync(struct myfs *myfs);

#endif /*__MYFS_H__*/
