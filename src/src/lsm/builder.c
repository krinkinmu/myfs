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
#include <lsm/ctree.h>
#include <myfs.h>

#include <string.h>
#include <stdlib.h>
#include <stddef.h>
#include <assert.h>


static void myfs_level_setup(struct myfs_ctree_level *level)
{
	memset(level, 0, sizeof(*level));
}

static void myfs_level_release(struct myfs_ctree_level *level)
{
	free(level->node);
	free(level->buf);
}

static void myfs_level_reset(struct myfs_ctree_level *level)
{
	level->size = 0;
	level->buf_size = 0;
}


void myfs_builder_setup(struct myfs_ctree_builder *builder)
{
	for (int i = 0; i <= MYFS_MAX_CTREE_HIGHT; ++i)
		myfs_level_setup(&builder->level[i]);
	memset(&builder->sb, 0, sizeof(builder->sb));
}

void myfs_builder_release(struct myfs_ctree_builder *builder)
{
	for (int i = 0; i <= MYFS_MAX_CTREE_HIGHT; ++i)
		myfs_level_release(&builder->level[i]);
	memset(&builder->sb, 0, sizeof(builder->sb));
}



static void myfs_level_reserve(struct myfs *myfs,
			struct myfs_ctree_level *level, size_t size)
{
	const size_t mb = (size_t)1024 * 1024;
	const size_t init = myfs_align_up(mb, myfs->page_size);

	while (level->buf_size + size > level->buf_cap) {
		const size_t cap = level->buf_cap ? level->buf_cap * 2 : init;

		assert(level->buf = realloc(level->buf, cap));
		level->buf_cap = cap;
	}
}

static void myfs_level_add(struct myfs *myfs, struct myfs_ctree_level *level,
			const void *data, size_t size)
{
	myfs_level_reserve(myfs, level, size);
	memcpy((char *)level->buf + level->buf_size, data, size);
	level->buf_size += size;
}

static void myfs_level_fill(struct myfs *myfs, struct myfs_ctree_level *level,
			int data, size_t size)
{
	myfs_level_reserve(myfs, level, size);
	memset((char *)level->buf + level->buf_size, data, size);
	level->buf_size += size;
}

static int myfs_buffer_full(const struct myfs *myfs,
			const struct myfs_ctree_buffer *buffer,
			size_t size)
{
	if (buffer->size < myfs->fanout)
		return 0;

	const size_t page_size = myfs->page_size;
	const size_t aligned = myfs_align_up(buffer->buf_size, page_size);

	if (aligned - buffer->buf_size >= size)
		return 0;

	return 1;
}

static void myfs_buffer_finish(struct myfs *myfs,
			struct myfs_ctree_builder *builder, size_t lvl)
{
	struct myfs_ctree_level *level = &builder->level[lvl];
	struct myfs_ctree_buffer *last = &level->node[level->size - 1];

	const size_t page_size = myfs->page_size;
	const size_t aligned = myfs_align_up(last->buf_size, page_size);
	const struct myfs_ctree_node_sb sb = { last->size, last->buf_size };

	struct __myfs_ctree_node_sb __sb;


	myfs_level_fill(myfs, level, 0, aligned - last->buf_size);
	myfs_ctree_node_sb2disk(&__sb, &sb);
	memcpy((char *)level->buf + last->buf_offs, &__sb, sizeof(__sb));
	last->buf_size = aligned;
}

static int myfs_level_append(struct myfs *myfs,
			struct myfs_ctree_builder *builder, size_t lvl,
			const struct myfs_key *key,
			const struct myfs_value *value);

static int myfs_level_flush(struct myfs *myfs,
			struct myfs_ctree_builder *builder, size_t lvl)
{
	struct myfs_ctree_level *level = &builder->level[lvl];

	if (!level->size)
		return 0;

	const uint64_t page_size = myfs->page_size;
	const uint64_t bytes = level->buf_size;
	const uint64_t pages = bytes / page_size;

	uint64_t offs;
	int ret = myfs_reserve(myfs, pages, &offs);

	if (ret)
		return ret;

	ret = myfs_block_write(myfs, level->buf, bytes, offs * page_size);
	if (ret)
		return ret;

	for (size_t i = 0; i != level->size; ++i) {
		const struct myfs_ctree_buffer *buffer = &level->node[i];
		const void *buf = (const char *)level->buf + buffer->buf_offs;
		const size_t bytes = buffer->buf_size;
		const size_t pages = bytes / page_size;
		const uint64_t csum = myfs_csum(buf, bytes);
		const struct myfs_ptr ptr = { offs, pages, csum };

		struct __myfs_ptr __ptr;
		struct myfs_key key;
		struct myfs_value value;


		offs += pages;

		key.size = buffer->key_size;
		key.data = (char *)level->buf + buffer->buf_offs +
					buffer->key_offs;

		myfs_ptr2disk(&__ptr, &ptr);
		value.size = sizeof(__ptr);
		value.data = &__ptr;

		ret = myfs_level_append(myfs, builder, lvl + 1,
					&key, &value);
		if (ret)
			return ret;
	}
	myfs_level_reset(level);
	builder->sb.size += pages;
	return 0;
}

static int myfs_buffer_add(struct myfs *myfs,
			struct myfs_ctree_builder *builder, size_t lvl)
{
	const size_t page_size = myfs->page_size;
	const size_t mb = (size_t)1024 * 1024;
	const size_t threshold = myfs_align_up(mb, page_size);
	struct myfs_ctree_level *level = &builder->level[lvl];

	if (level->size)
		myfs_buffer_finish(myfs, builder, lvl);

	if (level->buf_size >= threshold) {
		const int ret = myfs_level_flush(myfs, builder, lvl);

		if (ret)
			return ret;
	}

	if (level->size == level->cap) {
		const size_t cap = level->cap ? level->cap * 2 : 256;
		const size_t size = cap * sizeof(*level->node);

		assert(level->node = realloc(level->node, size));
		level->cap = cap;
	}

	struct myfs_ctree_buffer *buffer = &level->node[level->size++];

	memset(buffer, 0, sizeof(*buffer));
	buffer->buf_offs = level->buf_size;
	buffer->buf_size = sizeof(struct __myfs_ctree_node_sb);
	myfs_level_fill(myfs, level, 0, sizeof(struct __myfs_ctree_node_sb));
	return 0;
}

static int myfs_level_append(struct myfs *myfs,
			struct myfs_ctree_builder *builder, size_t lvl,
			const struct myfs_key *key,
			const struct myfs_value *value)
{
	const size_t size = sizeof(struct __myfs_ctree_item)
				+ key->size + value->size;

	struct myfs_ctree_level *level = &builder->level[lvl];
	struct myfs_ctree_buffer *buffer = &level->node[level->size - 1];

	if (!level->size || myfs_buffer_full(myfs, buffer, size)) {
		const int ret = myfs_buffer_add(myfs, builder, lvl);

		if (ret)
			return ret;
	}

	const struct myfs_ctree_item item = { key->size, value->size };
	struct __myfs_ctree_item __item;

	myfs_ctree_item2disk(&__item, &item);
	myfs_level_add(myfs, level, &__item, sizeof(__item));

	buffer = &level->node[level->size - 1];
	buffer->buf_size += sizeof(__item);
	buffer->key_offs = buffer->buf_size;
	buffer->key_size = key->size;
	buffer->value_offs = buffer->key_offs + buffer->key_size;
	buffer->value_size = value->size;

	myfs_level_add(myfs, level, key->data, key->size);
	myfs_level_add(myfs, level, value->data, value->size);
	buffer->buf_size += key->size + value->size;
	++buffer->size;
	if (builder->sb.hight < lvl)
		builder->sb.hight = lvl;
	return 0;
}

int myfs_builder_append(struct myfs *myfs, struct myfs_ctree_builder *builder,
			const struct myfs_key *key,
			const struct myfs_value *value)
{
	return myfs_level_append(myfs, builder, 0, key, value);
}

int myfs_builder_finish(struct myfs *myfs, struct myfs_ctree_builder *builder)
{
	struct myfs_ctree_sb *sb = &builder->sb;

	if (!sb->hight && !builder->level[0].size)
		return 0;

	for (size_t i = 0; i <= sb->hight; ++i) {
		const size_t h = sb->hight;
		const struct myfs_ctree_level *level = &builder->level[i];
		const struct myfs_ctree_buffer *buffer = level->node;

		if (h && i == h && level->size == 1 && buffer->size == 1)
			break;

		if (level->size) {
			myfs_buffer_finish(myfs, builder, i);

			const int ret = myfs_level_flush(myfs, builder, i);

			if (ret)
				return ret;
		}
	}

	const int hight = sb->hight;
	const struct myfs_ctree_level *level = &builder->level[hight];
	const struct myfs_ctree_buffer *buffer = &level->node[0];

	struct __myfs_ptr __ptr;

	assert(buffer->value_size == sizeof(__ptr));
	memcpy(&__ptr, (const char *)level->buf + buffer->buf_offs +
				buffer->value_offs, sizeof(__ptr));
	myfs_ptr2mem(&builder->sb.root, &__ptr);
	return 0;
}
