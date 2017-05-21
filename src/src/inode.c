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
#include <inode.h>
#include <myfs.h>

#include <stdlib.h>
#include <assert.h>
#include <errno.h>


void __myfs_icache_setup(struct myfs_icache *cache, size_t bits)
{
	assert(bits < 64);
	assert(cache->head = calloc(1ul << bits, sizeof(*cache->head)));
	assert(cache->mtx = calloc(1ul << bits, sizeof(*cache->mtx)));

	for (size_t i = 0; i != (1ul << bits); ++i)
		assert(!pthread_mutex_init(&cache->mtx[i], NULL));

	cache->bits = bits;
	cache->a = rand();
	cache->b = rand();
}

void myfs_icache_setup(struct myfs_icache *cache)
{
	__myfs_icache_setup(cache, 20);
}

void myfs_icache_release(struct myfs_icache *cache)
{
	size_t count = 0;
	for (size_t i = 0; i != (1ul << cache->bits); ++i) {
		struct hlist_head *head = &cache->head[i];
		struct hlist_node *node = head->head;

		while (node) {
			struct myfs_inode *inode = (struct myfs_inode *)node;

			++count;
			node = node->next;
			assert(!pthread_rwlock_destroy(&inode->rwlock));
			free(inode->bmap.entry);
			free(inode);
		}	
		assert(!pthread_mutex_destroy(&cache->mtx[i]));
	}
	free(cache->head);
	free(cache->mtx);
	memset(cache, 0, sizeof(*cache));
}


static size_t myfs_icache_index(const struct myfs_icache *cache, uint64_t ino)
{
	const uint64_t prime = 973078537;
	const uint64_t x = (ino * cache->a + cache->b) % prime;

	return x & ((1ul << cache->bits) - 1);
}

static struct myfs_inode *__myfs_inode_get(struct myfs_icache *cache,
			size_t bucket, uint64_t ino)
{
	struct hlist_head *head = &cache->head[bucket];
	struct hlist_node *node = head->head;

	while (node) {
		struct myfs_inode *inode = (struct myfs_inode *)node;

		if (inode->inode == ino)
			return inode;
		node = node->next;
	}

	struct myfs_inode *inode = malloc(sizeof(*inode));

	memset(inode, 0, sizeof(*inode));
	inode->inode = ino;
	inode->flags = MYFS_INODE_NEW;
	assert(!pthread_rwlock_init(&inode->rwlock, NULL));
	hlist_add(head, &inode->ll);
	return inode;
}

struct myfs_inode *myfs_inode_get(struct myfs *myfs, uint64_t ino)
{
	struct myfs_icache *cache = &myfs->icache;
	const size_t bucket = myfs_icache_index(cache, ino);
	struct myfs_inode *inode;

	assert(bucket < (1ul << cache->bits));
	assert(!pthread_mutex_lock(&cache->mtx[bucket]));
	assert(inode = __myfs_inode_get(cache, bucket, ino));
	++inode->refcnt;
	assert(!pthread_mutex_unlock(&cache->mtx[bucket]));
	return inode;
}

void __myfs_inode_put(struct myfs *myfs, struct myfs_inode *inode,
			unsigned long refcnt)
{
	struct myfs_icache *cache = &myfs->icache;
	const size_t bucket = myfs_icache_index(cache, inode->inode);
	int delete = 0;

	assert(bucket < (1ul << cache->bits));
	assert(!pthread_mutex_lock(&cache->mtx[bucket]));
	assert(inode->refcnt >= refcnt);
	inode->refcnt -= refcnt;
	if (!inode->refcnt) {
		hlist_del(&inode->ll);
		delete = 1;
	}
	assert(!pthread_mutex_unlock(&cache->mtx[bucket]));

	if (delete) {
		assert(!pthread_rwlock_destroy(&inode->rwlock));
		free(inode->bmap.entry);
		free(inode);
	}
}

void myfs_inode_put(struct myfs *myfs, struct myfs_inode *inode)
{
	if (!inode)
		return;
	__myfs_inode_put(myfs, inode, 1);
}


static void myfs_bmap_entry2disk(struct __myfs_bmap_entry *disk,
			const struct myfs_bmap_entry *mem)
{
	disk->disk_offs = htole64(mem->disk_offs);
	disk->file_offs = htole64(mem->file_offs);
}

static void myfs_bmap_entry2mem(struct myfs_bmap_entry *mem,
			const struct __myfs_bmap_entry *disk)
{
	mem->disk_offs = le64toh(disk->disk_offs);
	mem->file_offs = le64toh(disk->file_offs);
}

static void myfs_inode_key2disk(struct __myfs_inode_key *disk,
			const struct myfs_inode *mem)
{
	disk->inode = htole64(mem->inode);
}

static void myfs_inode_value2disk(struct __myfs_inode_value *disk,
			const struct myfs_inode *mem)
{
	disk->size = htole64(mem->size);
	disk->mtime = htole64(mem->mtime);
	disk->ctime = htole64(mem->ctime);
	disk->links = htole32(mem->links);
	disk->type = htole32(mem->type);
	disk->uid = htole32(mem->uid);
	disk->gid = htole32(mem->gid);
	disk->perm = htole32(mem->perm);
	disk->bmap.size = htole32(mem->bmap.size);
}

static void myfs_inode_key2mem(struct myfs_inode *mem,
			const struct __myfs_inode_key *disk)
{
	mem->inode = le64toh(disk->inode);
}

static void myfs_inode_value2mem(struct myfs_inode *mem,
			const struct __myfs_inode_value *disk)
{
	mem->size = le64toh(disk->size);
	mem->mtime = le64toh(disk->mtime);
	mem->ctime = le64toh(disk->ctime);
	mem->links = le32toh(disk->links);
	mem->type = le32toh(disk->type);
	mem->uid = le32toh(disk->uid);
	mem->gid = le32toh(disk->gid);
	mem->perm = le32toh(disk->perm);
	mem->bmap.size = le32toh(disk->bmap.size);
}


struct myfs_inode_query {
	struct myfs_query query;
	struct myfs_inode *inode;
};


static int myfs_inode_cmp(const struct myfs_inode *l,
			const struct myfs_inode *r)
{
	if (l->inode < r->inode)
		return -1;
	return l->inode > r->inode;
}

static int myfs_inode_lookup_cmp(struct myfs_query *q,
			const struct myfs_key *key)
{
	struct myfs_inode_query *query = (struct myfs_inode_query *)q;
	const struct __myfs_inode_key *__inode = key->data;
	struct myfs_inode inode;

	assert(key->size == sizeof(*__inode));
	myfs_inode_key2mem(&inode, __inode);
	return myfs_inode_cmp(&inode, query->inode);
}

static int myfs_inode_lookup_emit(struct myfs_query *q,
                        const struct myfs_key *key,
                        const struct myfs_value *value)
{
	struct myfs_inode_query *query = (struct myfs_inode_query *)q;
	struct myfs_inode *inode = query->inode;

	myfs_inode_key2mem(inode, key->data);
	myfs_inode_value2mem(inode, value->data);
	if (inode->type & MYFS_TYPE_DEL)
		return 0;

	const struct __myfs_inode_value *__inode = value->data;
	const struct __myfs_bmap *__bmap = &__inode->bmap;
	size_t size;

	size = inode->bmap.size * sizeof(inode->bmap.entry[0]);
	assert(inode->bmap.entry = malloc(size));
	for (size_t i = 0; i != inode->bmap.size; ++i)
		myfs_bmap_entry2mem(&inode->bmap.entry[i], &__bmap->entry[i]);
	return 1;
}


static void myfs_inode2entry(struct myfs_key *key, struct myfs_value *value,
			const struct myfs_inode *inode)
{
	const struct myfs_bmap *bmap = &inode->bmap;

	assert(key->data = malloc(sizeof(struct __myfs_inode_key)));
	myfs_inode_key2disk(key->data, inode);
	key->size = sizeof(struct __myfs_inode_key);

	const size_t ent_size = sizeof(struct __myfs_bmap_entry);
	const size_t size = sizeof(struct __myfs_inode_value) - ent_size +
				bmap->size * ent_size;
	struct __myfs_inode_value *__inode;
	struct __myfs_bmap *__bmap;

	assert(__inode = value->data = malloc(size));
	value->size = size;
	myfs_inode_value2disk(value->data, inode);
	__bmap = &__inode->bmap;
	for (size_t i = 0; i != bmap->size; ++i)
		myfs_bmap_entry2disk(&__bmap->entry[i], &bmap->entry[i]);
}

int __myfs_inode_write(struct myfs *myfs, struct myfs_inode *inode)
{
	struct myfs_value value;
	struct myfs_key key;
	int err;

	myfs_inode2entry(&key, &value, inode);
	err = myfs_lsm_insert(&myfs->inode_map, &key, &value);
	myfs_entry_release(&key, &value);
	if (!err)
		inode->flags &= ~MYFS_INODE_NEW;
	return err;
}

int __myfs_inode_read(struct myfs *myfs, struct myfs_inode *inode)
{
	struct myfs_inode_query query = {
		.query = {
			.cmp = &myfs_inode_lookup_cmp,
			.emit = &myfs_inode_lookup_emit,
		},
		.inode = inode,
	};

	if (!(inode->flags & MYFS_INODE_NEW))
		return 0;

	const int ret = myfs_lsm_lookup(&myfs->inode_map, &query.query);

	if (!ret)
		return -ENOENT;
	if (1 == ret) {
		inode->flags &= ~MYFS_INODE_NEW;
		return 0;
	}
	return ret;
}

int myfs_inode_read(struct myfs *myfs, struct myfs_inode *inode)
{
	int toread = 0;
	int ret;

	assert(!pthread_rwlock_rdlock(&inode->rwlock));
	if (inode->flags & MYFS_INODE_NEW)
		toread = 1;
	assert(!pthread_rwlock_unlock(&inode->rwlock));

	if (!toread)
		return 0;

	assert(!pthread_rwlock_wrlock(&inode->rwlock));
	ret = __myfs_inode_read(myfs, inode);
	assert(!pthread_rwlock_unlock(&inode->rwlock));
	return ret;
}


static int myfs_inode_key_cmp(const struct myfs_key *l,
			const struct myfs_key *r)
{
	struct myfs_inode left, right;

	assert(l->size >= sizeof(struct __myfs_inode_key));
	assert(r->size >= sizeof(struct __myfs_inode_key));
	myfs_inode_key2mem(&left, l->data);
	myfs_inode_key2mem(&right, r->data);
	return myfs_inode_cmp(&left, &right);
}

static int myfs_inode_key_deleted(const struct myfs_key *key,
			const struct myfs_value *value)
{
	const struct __myfs_inode_value *inode = value->data;
	le32_t type;

	(void) key;
	memcpy(&type, &inode->type, sizeof(type));
	return le32toh(type) & MYFS_TYPE_DEL;
}

void myfs_inode_map_setup(struct myfs_lsm *lsm, struct myfs *myfs,
			const struct myfs_lsm_sb *sb)
{
	static struct myfs_key_ops kops = {
		&myfs_inode_key_cmp,
		&myfs_inode_key_deleted
	};

	myfs_lsm_setup(lsm, myfs, &myfs_lsm_default_policy, &kops, sb);
}

void myfs_inode_map_release(struct myfs_lsm *lsm)
{
	myfs_lsm_release(lsm);
}
