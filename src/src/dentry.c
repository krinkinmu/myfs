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
#include <dentry.h>
#include <myfs.h>

#include <string.h>
#include <errno.h>


void myfs_dentry_key2disk(struct __myfs_dentry_key *disk,
			const struct myfs_dentry *mem)
{
	disk->parent = htole64(mem->parent);
	disk->hash = htole32(mem->hash);
	disk->size = htole32(mem->size);
	memcpy(disk->name, mem->name, mem->size);
}

void myfs_dentry_value2disk(struct __myfs_dentry_value *disk,
			const struct myfs_dentry *mem)
{
	disk->inode = htole64(mem->inode);
	disk->type = htole32(mem->type);
}

void myfs_dentry_key2mem(struct myfs_dentry *mem,
			const struct __myfs_dentry_key *disk)
{
	mem->parent = le64toh(disk->parent);
	mem->hash = le32toh(disk->hash);
	mem->size = le32toh(disk->size);
	mem->name = disk->name;
}

void myfs_dentry_value2mem(struct myfs_dentry *mem,
			const struct __myfs_dentry_value *disk)
{
	mem->inode = le64toh(disk->inode);
	mem->type = le32toh(disk->type);
}

static int myfs_dentry_cmp(const struct myfs_dentry *l,
			const struct myfs_dentry *r)
{
	if (l->parent != r->parent)
		return l->parent < r->parent ? -1 : 1;
	if (l->hash != r->hash)
		return l->hash < r->hash ? -1 : 1;
	if (l->size != r->size)
		return l->size < r->size ? -1 : 1;
	return memcmp(l->name, r->name, l->size);
}

static int myfs_dentry_key_cmp(const struct myfs_key *l,
			const struct myfs_key *r)
{
	struct myfs_dentry left, right;

	assert(l->size >= sizeof(struct __myfs_dentry_key));
	assert(r->size >= sizeof(struct __myfs_dentry_key));
	myfs_dentry_key2mem(&left, l->data);
	myfs_dentry_key2mem(&right, r->data);
	return myfs_dentry_cmp(&left, &right);
}

static int myfs_dentry_key_deleted(const struct myfs_key *key,
			const struct myfs_value *value)
{
	const struct __myfs_dentry_value *dentry = value->data;
	le32_t type;

	(void) key;
	memcpy(&type, &dentry->type, sizeof(type));
	return le32toh(type) & MYFS_TYPE_DEL;
}

void myfs_dentry_map_setup(struct myfs_lsm *lsm, struct myfs *myfs,
			const struct myfs_lsm_sb *sb)
{
	static struct myfs_key_ops kops = {
		&myfs_dentry_key_cmp,
		&myfs_dentry_key_deleted
	};

	myfs_lsm_setup(lsm, myfs, &myfs_lsm_default_policy, &kops, sb);
}

void myfs_dentry_map_release(struct myfs_lsm *lsm)
{
	myfs_lsm_release(lsm);
}



struct myfs_dentry_query {
	struct myfs_query query;
	const struct myfs_dentry *key;
	struct myfs_dentry *found;
};


static int myfs_dentry_lookup_cmp(struct myfs_query *q,
			const struct myfs_key *key)
{
	struct myfs_dentry_query *query = (struct myfs_dentry_query *)q;
	const struct __myfs_dentry_key *__key = key->data;
	struct myfs_dentry dentry;

	assert(key->size >= sizeof(*__key));
	myfs_dentry_key2mem(&dentry, __key);
	return myfs_dentry_cmp(&dentry, query->key);
}

static int myfs_dentry_lookup_emit(struct myfs_query *q,
			const struct myfs_key *key,
			const struct myfs_value *value)
{
	struct myfs_dentry_query *query = (struct myfs_dentry_query *)q;
	const struct __myfs_dentry_key *__key = key->data;
	const struct __myfs_dentry_value *__value = value->data;

	myfs_dentry_key2mem(query->found, __key);
	myfs_dentry_value2mem(query->found, __value);
	query->found->name = NULL;
	return (query->found->type & MYFS_TYPE_DEL) ? 0 : 1;
}


int myfs_dentry_read(struct myfs *myfs, struct myfs_inode *dir,
			const char *name, struct myfs_dentry *dentry)
{
	const size_t size = strlen(name);
	const struct myfs_dentry key = {
		.parent = dir->inode,
		.inode = 0,
		.hash = myfs_hash(name, size),
		.type = 0,
		.size = size,
		.name = name
	};
	struct myfs_dentry_query query = {
		.query = {
			.cmp = &myfs_dentry_lookup_cmp,
			.emit = &myfs_dentry_lookup_emit,
		},
		.key = &key,
		.found = dentry,
	};

	const int ret = myfs_lsm_lookup(&myfs->dentry_map, &query.query);

	if (!ret)
		return -ENOENT;
	if (1 == ret) {
		dentry->name = name;
		return 0;
	}
	return ret;
}


union myfs_dentry_key_wrap {
	struct __myfs_dentry_key key;
	char buf[sizeof(struct __myfs_dentry_key) + MYFS_FS_NAMEMAX];
};


int __myfs_dentry_write(struct myfs *myfs, const struct myfs_dentry *dentry)
{
	union myfs_dentry_key_wrap __key;
	struct __myfs_dentry_value __value;
	struct myfs_key key;
	struct myfs_value value;

	myfs_dentry_key2disk(&__key.key, dentry);
	key.data = &__key.key;
	key.size = sizeof(struct __myfs_dentry_key) + dentry->size - 1;

	myfs_dentry_value2disk(&__value, dentry);
	value.data = &__value;
	value.size = sizeof(__value);

	return myfs_lsm_insert(&myfs->dentry_map, &key, &value);
}
