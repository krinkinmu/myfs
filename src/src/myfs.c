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
#include <alloc/alloc.h>
#include <block/block.h>
#include <lsm/lsm.h>
#include <misc/xxhash.h>
#include <inode.h>
#include <dentry.h>
#include <myfs.h>

#include <sys/stat.h>
#include <unistd.h>

#include <stdatomic.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <errno.h>
#include <time.h>

#include <stdio.h>


uint64_t myfs_csum(const void *buf, size_t size)
{
	return XXH64(buf, size, MYFS_FS_MAGIC);
}

uint32_t myfs_hash(const void *buf, size_t size)
{
	return XXH32(buf, size, MYFS_FS_MAGIC);
}

uint64_t myfs_now(void)
{
	struct timespec spec;

	assert(!clock_gettime(CLOCK_REALTIME, &spec));
	return myfs_timespec2stamp(&spec);
}

static void *myfs_flusher(void *arg)
{
	const uint64_t delay = 60 * 1000;
	uint64_t last = myfs_now();
	struct myfs *myfs = arg;

	while (!atomic_load_explicit(&myfs->done, memory_order_relaxed)) {
		if (!myfs_lsm_need_flush(&myfs->inode_map)
				&& !myfs_lsm_need_flush(&myfs->dentry_map)
				&& (myfs_now() - last) < delay) {
			sleep(1);
			continue;
		}

		if (myfs_commit(myfs)) {
			sleep(1);
			continue;
		}

		last = myfs_now();

		for (int i = 0; i != MYFS_MAX_TREES; ++i) {
			if (!myfs_lsm_need_merge(&myfs->inode_map, i))
				continue;
			if (atomic_load_explicit(&myfs->done,
					memory_order_relaxed))
				break;
			myfs_lsm_merge(&myfs->inode_map, i);
		}

		for (int i = 0; i != MYFS_MAX_TREES; ++i) {
			if (!myfs_lsm_need_merge(&myfs->dentry_map, i))
				continue;
			if (atomic_load_explicit(&myfs->done,
					memory_order_relaxed))
				break;
			myfs_lsm_merge(&myfs->dentry_map, i);
		}
	}
	return NULL;
}

static int myfs_check_read(struct myfs *myfs, struct __myfs_check *check,
			size_t size, uint64_t offs)
{
	int ret = myfs_block_read(myfs, check, size, offs);

	if (ret)
		return ret;

	const uint64_t csum = le64toh(check->csum);

	check->csum = 0;
	if (csum != myfs_csum(check, size))
		ret = -EIO;
	check->csum = htole64(csum);
	return ret;
}


union myfs_sb_wrap {
	struct __myfs_sb sb;
	char buf[512];
};

static void __myfs_umount(struct myfs *myfs)
{
	assert(!pthread_rwlock_destroy(&myfs->trans_lock));
	assert(!pthread_mutex_destroy(&myfs->commit_lock));
	myfs_icache_release(&myfs->icache);
	myfs_dentry_map_release(&myfs->dentry_map);
	myfs_inode_map_release(&myfs->inode_map);
}

int myfs_mount(struct myfs *myfs, struct bdev *bdev)
{
	union myfs_sb_wrap sb;
	int ret;

	myfs->bdev = bdev;
	ret = myfs_block_read(myfs, &sb, sizeof(sb), 0);
	if (ret)
		return ret;

	myfs_sb2mem(&myfs->sb, &sb.sb);
	if (myfs->sb.magic != MYFS_FS_MAGIC)
		return -EIO;

	const size_t page_size = myfs->sb.page_size;
	const size_t size = myfs_align_up(bdev_size(bdev), page_size);

	const size_t check_size = myfs->sb.check_size * page_size;
	const uint64_t check_offs = myfs->sb.check_offs * page_size;
	const uint64_t bcheck_offs = myfs->sb.backup_check_offs * page_size;

	struct __myfs_check *check = malloc(check_size);

	assert(check);
	ret = myfs_check_read(myfs, check, check_size, check_offs);
	if (ret) {
		ret = myfs_check_read(myfs, check, check_size, bcheck_offs);
		if (ret) {
			free(check);
			return ret;
		}
	}

	myfs_check2mem(&myfs->check, check);
	free(check);

	myfs->page_size = page_size;
	myfs->fanout = MYFS_MIN_FANOUT;
	atomic_store_explicit(&myfs->next_offs, size / page_size,
				memory_order_relaxed);
	atomic_store_explicit(&myfs->next_ino, myfs->check.ino,
				memory_order_relaxed);

	myfs_inode_map_setup(&myfs->inode_map, myfs, &myfs->check.inode_sb);
	myfs_dentry_map_setup(&myfs->dentry_map, myfs, &myfs->check.dentry_sb);
	myfs_icache_setup(&myfs->icache);
	assert(!pthread_rwlock_init(&myfs->trans_lock, NULL));
	assert(!pthread_mutex_init(&myfs->commit_lock, NULL));
	atomic_store_explicit(&myfs->done, 0, memory_order_relaxed);

	myfs->root = myfs_inode_get(myfs, MYFS_FS_ROOT);
	ret = __myfs_inode_read(myfs, myfs->root);
	if (ret) {
		__myfs_umount(myfs);
		return ret;
	}
	/* fuse sometimes calls forget for the root inode, even though
	   root inode counter can't actually be incremented. */
	++myfs->root->refcnt;
	assert(!pthread_create(&myfs->flusher, NULL, &myfs_flusher, myfs));

	return 0;
}

void myfs_unmount(struct myfs *myfs)
{
	atomic_store_explicit(&myfs->done, 1, memory_order_relaxed);
	assert(!pthread_join(myfs->flusher, NULL));	
	__myfs_umount(myfs);
	memset(myfs, 0, sizeof(*myfs));
}

int myfs_checkpoint(struct myfs *myfs)
{
	const size_t page_size = myfs->page_size;
	const size_t check_size = myfs->sb.check_size * page_size;
	const uint64_t check_offs = myfs->sb.check_offs * page_size;
	const uint64_t bcheck_offs = myfs->sb.backup_check_offs * page_size;

	struct __myfs_check *check = malloc(check_size);

	assert(check);
	memset(check, 0, check_size);

	myfs_lsm_get_root(&myfs->check.inode_sb, &myfs->inode_map);
	myfs_lsm_get_root(&myfs->check.dentry_sb, &myfs->dentry_map);
	++myfs->check.gen;
	myfs_check2disk(check, &myfs->check);

	const uint64_t csum = myfs_csum(check, check_size);
	int ret;

	check->csum = htole64(csum);
	do {
		ret = myfs_block_sync(myfs);
		if (ret)
			break;
		ret = myfs_block_write(myfs, check, check_size, check_offs);
		if (ret)
			break;
		ret = myfs_block_sync(myfs);
		if (ret)
			break;
		ret = myfs_block_write(myfs, check, check_size, bcheck_offs);
		if (ret)
			break;
	} while (0);

	free(check);
	return ret;
}

int myfs_commit(struct myfs *myfs)
{
	int err1, err2;

	assert(!pthread_mutex_lock(&myfs->commit_lock));
	assert(!pthread_rwlock_wrlock(&myfs->trans_lock));
	/* TODO: actually these calls may fail if previous flush failed
	   and we have nonempty c1 tree left, we need to at least try
	   to write nonempty c1 */
	assert(!myfs_lsm_flush_start(&myfs->inode_map));
	assert(!myfs_lsm_flush_start(&myfs->dentry_map));
	assert(!pthread_rwlock_unlock(&myfs->trans_lock));

	err1 = myfs_lsm_flush_finish(&myfs->inode_map);
	err2 = myfs_lsm_flush_finish(&myfs->dentry_map);
	if (err1 || err2) {
		assert(!pthread_mutex_unlock(&myfs->commit_lock));
		return err1 ? err1 : err2;
	}
	err1 = myfs_checkpoint(myfs);
	assert(!pthread_mutex_unlock(&myfs->commit_lock));
	return err1;
}


int myfs_lookup(struct myfs *myfs, struct myfs_inode *dir, const char *name,
			struct myfs_inode **inode)
{
	struct myfs_inode *child = NULL;
	struct myfs_dentry dentry;
	int ret = 0;

	assert(!pthread_rwlock_rdlock(&dir->rwlock));
	if (dir->type & MYFS_TYPE_DEL)
		ret = -ENOENT;
	if (!ret)
		ret = myfs_dentry_read(myfs, dir, name, &dentry);
	if (!ret) {
		child = myfs_inode_get(myfs, dentry.inode);
		ret = myfs_inode_read(myfs, child);
		if (ret) {
			myfs_inode_put(myfs, child);
			child = NULL;
		}
	}
	assert(!pthread_rwlock_unlock(&dir->rwlock));
	*inode = child;
	return ret;
}

static int __myfs_create(struct myfs *myfs,
			struct myfs_inode *dir, const char *name,
			uid_t uid, gid_t gid, mode_t mode,
			struct myfs_inode **inode)
{
	const uint64_t ino = atomic_fetch_add_explicit(&myfs->next_ino, 1,
				memory_order_relaxed);
	struct myfs_inode *child = myfs_inode_get(myfs, ino);
	struct myfs_dentry dentry;
	int ret = 0;

	const uint64_t time = myfs_now();
	const size_t len = strlen(name);


	assert(child && (child->flags & MYFS_INODE_NEW));
	child->inode = ino;
	child->links = 1;
	child->mtime = time;
	child->ctime = time;
	child->type = mode & S_IFMT;
	child->uid = uid;
	child->gid = gid;
	child->perm = mode & (S_IRWXU | S_IRWXG | S_IRWXO);

	ret = __myfs_inode_write(myfs, child);
	if (ret) {
		myfs_inode_put(myfs, child);
		return ret;
	}

	++dir->size;
	dir->mtime = myfs_now();
	ret = __myfs_inode_write(myfs, dir);
	if (ret) {
		myfs_inode_put(myfs, child);
		return ret;
	}	

	dentry.parent = dir->inode;
	dentry.inode = ino;
	dentry.hash = myfs_hash(name, len);
	dentry.type = mode & S_IFMT;
	dentry.size = len;
	dentry.name = name;

	ret = __myfs_dentry_write(myfs, &dentry);
	if (ret) {
		myfs_inode_put(myfs, child);
		return ret;
	}

	child->flags &= ~MYFS_INODE_NEW;
	*inode = child;
	return 0;
}

int myfs_create(struct myfs *myfs, struct myfs_inode *dir, const char *name,
			uid_t uid, gid_t gid, mode_t mode,
			struct myfs_inode **inode)
{
	struct myfs_dentry dentry;
	int err;

	if (strlen(name) > MYFS_FS_NAMEMAX)
		return -ENAMETOOLONG;

	assert(!pthread_rwlock_wrlock(&dir->rwlock));
	do {
		if (dir->type & MYFS_TYPE_DEL) {
			err = -ENOENT;
			break;
		}
		err = myfs_dentry_read(myfs, dir, name, &dentry);
		if (!err) {
			err = -EEXIST;
			break;
		}
		if (err != -ENOENT)
			break;

		myfs_trans_start(myfs);
		err = __myfs_create(myfs, dir, name, uid, gid, mode, inode);
		myfs_trans_finish(myfs);
	} while (0);
	assert(!pthread_rwlock_unlock(&dir->rwlock));
	return err;
}

static int __myfs_unlink(struct myfs *myfs, struct myfs_inode *dir,
			struct myfs_inode *inode,
			struct myfs_dentry *dentry)
{
	int ret = 0;

	if (--inode->links == 0)
		inode->type |= MYFS_TYPE_DEL;

	inode->mtime = myfs_now();
	ret = __myfs_inode_write(myfs, inode);
	if (ret)
		return ret;

	dentry->type |= MYFS_TYPE_DEL;
	ret = __myfs_dentry_write(myfs, dentry);
	if (ret)
		return ret;

	--dir->size;
	dir->mtime = myfs_now();
	ret = __myfs_inode_write(myfs, dir);
	return ret;
}

int myfs_unlink(struct myfs *myfs, struct myfs_inode *dir, const char *name)
{
	struct myfs_dentry dentry;
	struct myfs_inode *inode;
	int err;

	if (strlen(name) > MYFS_FS_NAMEMAX)
		return -ENAMETOOLONG;

	assert(!pthread_rwlock_wrlock(&dir->rwlock));
	do {
		if (dir->type & MYFS_TYPE_DEL) {
			err = -ENOENT;
			break;
		}

		if ((err = myfs_dentry_read(myfs, dir, name, &dentry)))
			break;

		assert(dir->size);
		assert(inode = myfs_inode_get(myfs, dentry.inode));
		err = myfs_inode_read(myfs, inode);
		if (!err) {
			assert(!pthread_rwlock_wrlock(&inode->rwlock));
			assert(!(inode->type & MYFS_TYPE_DEL));
			myfs_trans_start(myfs);
			err = __myfs_unlink(myfs, dir, inode, &dentry);
			myfs_trans_finish(myfs);
			assert(!pthread_rwlock_unlock(&inode->rwlock));
		}
		myfs_inode_put(myfs, inode);
	} while (0);
	assert(!pthread_rwlock_unlock(&dir->rwlock));
	return err;
}

int myfs_rmdir(struct myfs *myfs, struct myfs_inode *dir, const char *name)
{
	struct myfs_dentry dentry;
	struct myfs_inode *inode;
	int err;

	if (strlen(name) > MYFS_FS_NAMEMAX)
		return -ENAMETOOLONG;

	assert(!pthread_rwlock_wrlock(&dir->rwlock));
	do {
		if (dir->type & MYFS_TYPE_DEL) {
			err = -ENOENT;
			break;
		}

		if ((err = myfs_dentry_read(myfs, dir, name, &dentry)))
			break;

		assert(dir->size);

		if (!S_ISDIR(dentry.type)) {
			err = -ENOTDIR;
			break;
		}

		assert(inode = myfs_inode_get(myfs, dentry.inode));
		err = myfs_inode_read(myfs, inode);

		assert(!pthread_rwlock_wrlock(&inode->rwlock));
		assert(!(inode->type & MYFS_TYPE_DEL));
		if (!err && inode->size)
			err = -EBUSY;
		if (!err) {
			myfs_trans_start(myfs);
			err = __myfs_unlink(myfs, dir, inode, &dentry);
			myfs_trans_finish(myfs);
		}
		assert(!pthread_rwlock_unlock(&inode->rwlock));
		myfs_inode_put(myfs, inode);
	} while (0);
	assert(!pthread_rwlock_unlock(&dir->rwlock));
	return err;
}

static int __myfs_link(struct myfs *myfs, struct myfs_inode *inode,
			struct myfs_inode *dir, const char *name)
{
	const size_t len = strlen(name);

	struct myfs_dentry dentry;
	int ret = 0;

	++inode->links;
	inode->mtime = myfs_now();
	ret = __myfs_inode_write(myfs, inode);
	if (ret)
		return ret;

	++dir->size;
	dir->mtime = myfs_now();
	ret = __myfs_inode_write(myfs, dir);
	if (ret)
		return ret;

	dentry.parent = dir->inode;
	dentry.inode = inode->inode;
	dentry.hash = myfs_hash(name, len);
	dentry.type = inode->type;
	dentry.size = len;
	dentry.name = name;

	return __myfs_dentry_write(myfs, &dentry);
}

int myfs_link(struct myfs *myfs, struct myfs_inode *inode,
			struct myfs_inode *dir, const char *name)
{
	struct myfs_dentry dentry;
	int err;

	if (strlen(name) > MYFS_FS_NAMEMAX)
		return -ENAMETOOLONG;

	assert(!pthread_rwlock_wrlock(&dir->rwlock));
	do {
		if (dir->type & MYFS_TYPE_DEL) {
			err = -ENOENT;
			break;
		}

		err = myfs_dentry_read(myfs, dir, name, &dentry);
		if (!err) {
			err = -EEXIST;
			break;
		}
		if (err != -ENOENT)
			break;

		assert(!pthread_rwlock_wrlock(&inode->rwlock));
		if (inode->type & MYFS_TYPE_DEL)
			err = -ENOENT;
		else {
			myfs_trans_start(myfs);
			err = __myfs_link(myfs, inode, dir, name);
			myfs_trans_finish(myfs);
		}
		assert(!pthread_rwlock_unlock(&inode->rwlock));
		break;
	} while (0);
	assert(!pthread_rwlock_unlock(&dir->rwlock));
	return err;
}

static int __myfs_rename(struct myfs *myfs, struct myfs_inode *old,
			const char *oldname, struct myfs_inode *new,
			const char *newname)
{
	struct myfs_dentry oldentry, newentry;
	struct myfs_inode *unlink = NULL;
	struct myfs_inode *link = NULL;
	int err;

	err = myfs_dentry_read(myfs, old, oldname, &oldentry);
	if (err)
		return err;

	err = myfs_dentry_read(myfs, new, newname, &newentry);
	if (err && err != -ENOENT)
		return err;

	if (!err) {
		unlink = myfs_inode_get(myfs, newentry.inode);
		err = myfs_inode_read(myfs, unlink);
		if (err) {
			myfs_inode_put(myfs, unlink);
			return err;
		}
	}

	link = myfs_inode_get(myfs, oldentry.inode);
	err = myfs_inode_read(myfs, link);
	if (err) {
		myfs_inode_put(myfs, unlink);
		myfs_inode_put(myfs, link);
		return err;
	}

	// special case: the entry in the new points to the same inode
	// as the entry in the old, no need to do anything.
	if (unlink && unlink == link) {
		myfs_inode_put(myfs, unlink);
		myfs_inode_put(myfs, link);
		return 0;
	}

	assert(!pthread_rwlock_wrlock(&link->rwlock));
	if (unlink)
		assert(!pthread_rwlock_wrlock(&unlink->rwlock));

	if (link->type & MYFS_TYPE_DEL)
		err = -ENOENT;
	else {
		myfs_trans_start(myfs);
		// TODO: currently if a rename fails we must fail the whole
		//       filesystem, since the rename might have been partially
		//       applied, WAL will solve the problem.
		if (unlink && !(unlink->type & MYFS_TYPE_DEL))
			err = __myfs_unlink(myfs, new, unlink, &newentry);
		if (!err)
			err = __myfs_link(myfs, link, new, newname);
		if (!err)
			err = __myfs_unlink(myfs, old, link, &oldentry);
		myfs_trans_finish(myfs);
	}

	assert(!pthread_rwlock_unlock(&link->rwlock));
	if (unlink)
		assert(!pthread_rwlock_unlock(&unlink->rwlock));

	myfs_inode_put(myfs, unlink);
	myfs_inode_put(myfs, link);
	return err;
}

int myfs_rename(struct myfs *myfs, struct myfs_inode *old, const char *oldname,
			struct myfs_inode *new, const char *newname)
{
	int err;

	if (strlen(newname) > MYFS_FS_NAMEMAX)
		return -ENAMETOOLONG;

	if (old->inode == new->inode)
		assert(!pthread_rwlock_wrlock(&old->rwlock));
	else {
		if (old->inode < new->inode) {
			assert(!pthread_rwlock_wrlock(&old->rwlock));
			assert(!pthread_rwlock_wrlock(&new->rwlock));
		} else {
			assert(!pthread_rwlock_wrlock(&new->rwlock));
			assert(!pthread_rwlock_wrlock(&old->rwlock));
		}
	}

	if ((old->type & MYFS_TYPE_DEL) || (new->type & MYFS_TYPE_DEL))
		err = -ENOENT;

	if (!err)
		err = __myfs_rename(myfs, old, oldname, new, newname);

	if (old->inode == new->inode)
		assert(!pthread_rwlock_unlock(&old->rwlock));
	else {
		assert(!pthread_rwlock_unlock(&old->rwlock));
		assert(!pthread_rwlock_unlock(&new->rwlock));
	}
	return err;
}


struct myfs_readdir_query {
	struct myfs_query query;
	struct myfs_readdir_ctx *ctx;
	uint64_t parent;
	uint64_t cookie;
};


static int myfs_readdir_emit(struct myfs_query *q, const struct myfs_key *key,
			const struct myfs_value *value)
{
	struct myfs_readdir_query *query = (struct myfs_readdir_query *)q;
	struct myfs_dentry dentry;

	myfs_dentry_key2mem(&dentry, key->data);
	myfs_dentry_value2mem(&dentry, value->data);
	assert(dentry.parent == query->parent);
	if (dentry.type & MYFS_TYPE_DEL)
		return 0;
	return query->ctx->emit(query->ctx, &dentry);
}

static int myfs_readdir_cmp(struct myfs_query *q, const struct myfs_key *key)
{
	struct myfs_readdir_query *query = (struct myfs_readdir_query *)q;
	struct myfs_dentry dentry;

	myfs_dentry_key2mem(&dentry, key->data);
	if (dentry.parent != query->parent)
		return dentry.parent < query->parent ? -1 : 1;
	if (dentry.hash <= query->cookie)
		return -1;
	return 0;
}

int myfs_readdir(struct myfs *myfs, struct myfs_inode *inode,
			struct myfs_readdir_ctx *ctx, uint64_t cookie)
{
	struct myfs_readdir_query query = {
		.query = {
			.emit = &myfs_readdir_emit,
			.cmp = &myfs_readdir_cmp,
		},
		.ctx = ctx,
		.parent = inode->inode,
		.cookie = cookie
	};
	int err;

	assert(!pthread_rwlock_rdlock(&inode->rwlock));
	if (inode->type & MYFS_TYPE_DEL)
		err = -ENOENT;
	else
		err = myfs_lsm_range(&myfs->dentry_map, &query.query);
	assert(!pthread_rwlock_unlock(&inode->rwlock));
	return err;
}

static int __myfs_read(struct myfs *myfs, struct myfs_inode *inode,
			char *buf, size_t size, off_t off)
{
	const uint64_t page_size = myfs->page_size;

	// This helper can only handle page aligned reads
	assert(!(size & (page_size - 1)));
	assert(!(off & (page_size - 1)));

	off /= page_size;
	size /= page_size;

	// TODO: file content with non zero probability is contigous on the
	//       disk, so it might be efficient to merge contigous pages and
	//       read them all at once
	for (size_t i = 0; i != inode->bmap.size; ++i) {
		const struct myfs_bmap_entry *entry = &inode->bmap.entry[i];
		const uint64_t file_off = entry->file_offs;
		const uint64_t disk_off = entry->disk_offs;

		if (file_off >= (uint64_t)off + size)
			break;

		if (file_off < (uint64_t)off)
			continue;

		const int err = myfs_block_read(myfs,
					buf + page_size * (file_off - off),
					page_size, disk_off * page_size);

		if (err)
			return err;
	}
	return 0;
}

long myfs_read(struct myfs *myfs, struct myfs_inode *inode,
			void *data, size_t size, off_t off)
{
	const uint64_t page_size = myfs->page_size;
	const uint64_t from = myfs_align_down(off, page_size);
	const uint64_t to = myfs_align_up(off + size, page_size);
	const size_t aligned = to - from;
	char *buf = malloc(aligned);
	uint64_t file_size;
	long ret;

	assert(buf);
	memset(buf, 0, aligned);
	assert(!pthread_rwlock_wrlock(&inode->rwlock));
	do {
		if (inode->type & MYFS_TYPE_DEL) {
			ret = -ENOENT;
			break;
		}

		file_size = inode->size;
		ret = __myfs_read(myfs, inode, buf, aligned, from);
	} while (0);
	assert(!pthread_rwlock_unlock(&inode->rwlock));

	if (!ret) {
		memcpy(data, buf + off - from, size);
		ret = size;
		if (file_size < off + size)
			ret = file_size - off;
	}
	free(buf);
	return ret;
}

static int __myfs_write(struct myfs *myfs, struct myfs_inode *inode,
			const char *buf, size_t size, off_t off)
{
	const uint64_t page_size = myfs->page_size;

	// This helper can only handle page aligned reads
	assert(!(size & (page_size - 1)));
	assert(!(off & (page_size - 1)));

	size /= page_size;
	off /= page_size;

	uint64_t doff;
	int err = myfs_reserve(myfs, size, &doff);

	if (err)
		return err;

	err = myfs_block_write(myfs, buf, size * page_size, doff * page_size);
	if (err)
		return err;


	size_t from, to;

	for (from = 0; from != inode->bmap.size; ++from) {
		const struct myfs_bmap_entry *entry = &inode->bmap.entry[from];
		const uint64_t file_off = entry->file_offs;

		if (file_off >= (uint64_t)off)
			break;
	}

	for (to = from; to != inode->bmap.size; ++to) {
		const struct myfs_bmap_entry *entry = &inode->bmap.entry[to];
		const uint64_t file_off = entry->file_offs;

		if (file_off >= off + size)
			break;
	}

	struct myfs_bmap *bmap = &inode->bmap;
	const size_t count = bmap->size - (to - from) + size;
	const size_t entsize = sizeof(bmap->entry[0]);
	const size_t alloc = count * entsize;

	assert(bmap->entry = realloc(bmap->entry, alloc));
	memmove(&bmap->entry[from + size], &bmap->entry[to],
				(bmap->size - to) * entsize);
	bmap->size = count;

	for (size_t i = from; i != from + size; ++i) {
		struct myfs_bmap_entry *entry = &inode->bmap.entry[i];

		entry->disk_offs = doff++;
		entry->file_offs = off++;
	}

	return 0;
}

long myfs_write(struct myfs *myfs, struct myfs_inode *inode,
			const void *data, size_t size, off_t off)
{
	const uint64_t page_size = myfs->page_size;
	const uint64_t from = myfs_align_down(off, page_size);
	const uint64_t to = myfs_align_up(off + size, page_size);
	const size_t aligned = to - from;
	char *buf = malloc(aligned);
	long ret = 0;

	assert(buf);
	memset(buf, 0, aligned);
	assert(!pthread_rwlock_wrlock(&inode->rwlock));
	do {
		if (inode->type & MYFS_TYPE_DEL) {
			ret = -ENOENT;
			break;
		}

		if (from != (uint64_t)off) {
			ret = __myfs_read(myfs, inode, buf, page_size, from);
			if (ret)
				break;
		}

		if (to != (uint64_t)off + size) {
			ret = __myfs_read(myfs, inode,
						buf + aligned - page_size,
						page_size,
						to - page_size);
			if (ret)
				break;
		}
		
		memcpy(buf + off - from, data, size);

		// We don't need actualy to take __myfs_write inside transaction
		// in the current implementation, but we will need it when myfs
		// starts to track free/used space
		myfs_trans_start(myfs);
		ret = __myfs_write(myfs, inode, buf, aligned, from);
		if (ret) {
			myfs_trans_finish(myfs);
			break;
		}

		if (inode->size < (uint64_t)off + size)
			inode->size = (uint64_t)off + size;

		inode->mtime = myfs_now();
		ret = __myfs_inode_write(myfs, inode);
		myfs_trans_finish(myfs);

		if (ret)
			break;
		ret = size;
	} while (0);
	assert(!pthread_rwlock_unlock(&inode->rwlock));
	free(buf);
	return ret;
}


int myfs_block_write(struct myfs *myfs, const void *buf, uint64_t size,
			uint64_t offs)
{
	struct bio bio;
	int err;

	bio_setup(&bio, myfs->bdev);
	bio.flags = BIO_WRITE;
	bio_add_vec(&bio, (void *)buf, offs, size);
	bio_submit(&bio);
	bio_wait(&bio);
	err = bio.err;
	bio_release(&bio);
	return err;
}

int myfs_block_read(struct myfs *myfs, void *buf, uint64_t size, uint64_t offs)
{
	struct bio bio;
	int err;

	bio_setup(&bio, myfs->bdev);
	bio.flags = BIO_READ;
	bio_add_vec(&bio, buf, offs, size);
	bio_submit(&bio);
	bio_wait(&bio);
	err = bio.err;
	bio_release(&bio);
	return err;
}

int myfs_block_sync(struct myfs *myfs)
{
	struct bio bio;
	int err;

	bio_setup(&bio, myfs->bdev);
	bio.flags = BIO_WRITE | BIO_SYNC;
	bio_submit(&bio);
	bio_wait(&bio);
	err = bio.err;
	bio_release(&bio);
	return err;
}
