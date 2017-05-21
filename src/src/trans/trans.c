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
#include <trans/trans.h>
#include <alloc/alloc.h>
#include <myfs.h>

#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>


void myfs_trans_setup(struct myfs_trans *trans)
{
	memset(trans, 0, sizeof(*trans));
}

void myfs_trans_release(struct myfs_trans *trans)
{
	free(trans->slot);
}


int myfs_trans_parse(struct myfs *myfs, struct myfs_trans *trans,
			const struct myfs_ptr *ptr)
{
	const uint64_t page_size = myfs->page_size;
	const uint64_t size = ptr->size * page_size;
	const uint64_t offs = ptr->offs * page_size;

	void *buf = malloc(ptr->size * page_size);

	assert(buf);

	int ret = myfs_block_read(myfs, buf, size, offs);

	if (ret) {
		free(buf);
		return ret;
	}

	if (myfs_csum(buf, size) != ptr->csum) {
		free(buf);
		return -EIO;
	}

	const struct __myfs_trans_sb *__sb = buf;
	const struct __myfs_ptr *__ptr = (const struct __myfs_ptr *)(__sb + 1);

	struct myfs_trans_sb sb;

	myfs_trans_sb2mem(&sb, __sb);
	trans->trans_id = sb.trans_id;
	trans->size = sb.slots;
	trans->ptr = *ptr;

	trans->slot = calloc(trans->size, sizeof(*trans->slot));
	assert(trans->slot);

	for (size_t i = 0; i != trans->size; ++i)
		myfs_ptr2mem(&trans->slot[i].ptr, &__ptr[i]);

	free(buf);
	return 0;
}

static int __myfs_slot_scan(const void *buf, struct myfs_trans_scanner *scanner)
{
	const struct __myfs_slot_sb *__sb = buf;
	const char *ptr = (const char *)(__sb + 1);

	struct myfs_slot_sb sb;
	int ret = 0;

	myfs_slot_sb2mem(&sb, __sb);
	for (size_t i = 0; !ret && i != sb.items; ++i) {
		const struct __myfs_log_item *__item =
					(const struct __myfs_log_item *)ptr;
		struct myfs_log_item item;

		myfs_log_item2mem(&item, __item);
		ptr += sizeof(*__item);

		const struct myfs_key key = {
			.size = item.key_size,
			.data = (void *)ptr
		};
		ptr += item.key_size;

		const struct myfs_value value = {
			.size = item.value_size,
			.data = (void *)ptr
		};
		ptr += item.value_size;

		ret = scanner->emit(scanner, item.type, &key, &value);
	}
	return ret;
}

static int myfs_slot_scan(struct myfs *myfs, const struct myfs_ptr *ptr,
			struct myfs_trans_scanner *scanner)
{
	const uint64_t page_size = myfs->page_size;
	const uint64_t offs = ptr->offs * page_size;
	const uint64_t size = ptr->size * page_size;

	void *buf = malloc(ptr->size * page_size);

	assert(buf);

	int ret = myfs_block_read(myfs, buf, size, offs);

	if (ret) {
		free(buf);
		return ret;
	}

	if (myfs_csum(buf, size) != ptr->csum) {
		free(buf);
		return -EIO;
	}

	ret = __myfs_slot_scan(buf, scanner);
	free(buf);
	return ret;
}

int myfs_trans_scan(struct myfs *myfs, const struct myfs_trans *trans,
			struct myfs_trans_scanner *scanner)
{
	for (size_t i = 0; i != trans->size; ++i) {
		const struct myfs_ptr *ptr = &trans->slot[i].ptr;
		const int ret = myfs_slot_scan(myfs, ptr, scanner);

		if (ret)
			return ret;
	}
	return 0;
}


void myfs_log_setup(struct myfs_log *log)
{
	memset(log, 0, sizeof(*log));
}

void myfs_log_release(struct myfs_log *log)
{
	free(log->ptr);
	free(log->data);
}


static void myfs_log_add(struct myfs_log *log, const struct myfs_ptr *ptr)
{
	if (log->size == log->cap) {
		const size_t cap = log->cap ? log->cap * 2 : 128;
		const size_t size = cap * sizeof(*ptr);

		assert(log->ptr = realloc(log->ptr, size));
		log->cap = cap;
	}

	log->ptr[log->size++] = *ptr;
}

static int myfs_log_flush(struct myfs *myfs, struct myfs_log *log)
{
	const uint64_t page_size = myfs->page_size;
	const uint64_t size = myfs_align_up(log->buf_sz, page_size);
	const uint64_t pages = size / page_size;

	if (!pages)
		return 0;

	uint64_t offs;
	int ret = myfs_reserve(myfs, pages, &offs);

	if (ret)
		return ret;

	const struct myfs_slot_sb sb = { .items = log->buf_entries };
	struct __myfs_slot_sb *__sb = log->data;

	myfs_slot_sb2disk(__sb, &sb);
	ret = myfs_block_write(myfs, log->data, size, offs * page_size);
	if (ret)
		return ret;

	const struct myfs_ptr ptr = {
		.size = pages,
		.offs = offs,
		.csum = myfs_csum(log->data, size)
	};
	myfs_log_add(log, &ptr);
	return 0;
}

int myfs_log_append(struct myfs *myfs, struct myfs_log *log,
			unsigned type,
			const struct myfs_key *key,
			const struct myfs_value *value)
{
	const size_t size = sizeof(struct __myfs_log_item) + key->size
				+ value->size;

	if (!log->data) {
		const size_t MYFS_MAX_LOG_BUF = ((size_t)1 << 20);

		assert(log->data = malloc(MYFS_MAX_LOG_BUF));
		log->buf_sz = sizeof(struct __myfs_slot_sb);
		log->buf_cap = MYFS_MAX_LOG_BUF;
		log->buf_entries = 0;
		memset(log->data, 0, log->buf_cap);
	}

	if (log->buf_sz + size > log->buf_cap) {
		const int ret = myfs_log_flush(myfs, log);

		if (ret)
			return ret;

		memset(log->data, 0, log->buf_cap);
		log->buf_sz = sizeof(struct __myfs_slot_sb);
		log->buf_entries = 0;
	}

	struct __myfs_log_item *__item = (struct __myfs_log_item *)
				((char *)log->data + log->buf_sz);
	char *ptr = (char *)(__item + 1);
	const struct myfs_log_item item = {
		.type = type,
		.key_size = key->size,
		.value_size = value->size
	};

	myfs_log_item2disk(__item, &item);
	memcpy(ptr, key->data, key->size);
	memcpy(ptr + key->size, value->data, value->size);
	log->buf_sz += size;
	++log->buf_entries;
	return 0;
}

int myfs_log_finish(struct myfs *myfs, struct myfs_log *log)
{
	const int ret = myfs_log_flush(myfs, log);

	free(log->data);
	log->data = NULL;
	return ret;
}

int myfs_log_scan(struct myfs *myfs, struct myfs_log *log,
			struct myfs_trans_scanner *scanner)
{
	for (size_t i = 0; i != log->size; ++i) {
		const struct myfs_ptr *ptr = &log->ptr[i];
		const int ret = myfs_slot_scan(myfs, ptr, scanner);

		if (ret)
			return ret;
	}
	return 0;
}


void myfs_log_register(struct myfs_trans *trans, struct myfs_log *log)
{
	const size_t size = trans->size + log->size;

	log->trans_id = trans->trans_id;
	log->offs = trans->size;

	trans->slot = realloc(trans->slot, size * sizeof(*trans->slot));
	assert(trans->slot);
	trans->size = size;

	for (size_t i = 0; i != log->size; ++i) {
		const size_t pos = i + log->offs;

		trans->slot[pos].ptr = log->ptr[i];
		trans->slot[pos].flags = 0;
	}
}

void myfs_log_commit(struct myfs_trans *trans, struct myfs_log *log)
{
	assert(trans->trans_id == log->trans_id);

	for (size_t i = log->offs; i != log->size + log->offs; ++i)
		trans->slot[i].flags |= MYFS_TRANS_REPLAYED;
}
