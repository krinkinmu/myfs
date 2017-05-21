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

#include <endian.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>


static int myfs_node_read(struct myfs *myfs,
			struct myfs_ctree_node *node,
			const struct myfs_ptr *ptr)
{
	if (!memcmp(&node->ptr, ptr, sizeof(*ptr)))
		return 0;

	const uint64_t page_size = myfs->page_size;
	const uint64_t offs = ptr->offs * page_size;
	const uint64_t size = ptr->size * page_size;
	int err = 0;


	assert(node->buf = realloc(node->buf, size));
	err = myfs_block_read(myfs, node->buf, size, offs);
	if (err)
		return err;

	if (myfs_csum(node->buf, size) != ptr->csum)
		return -EIO;

	struct __myfs_ctree_node_sb *__sb = node->buf;
	char *pos = (char *)(__sb + 1);


	myfs_ctree_node_sb2mem(&node->sb, __sb);
	node->ptr = *ptr;

	assert(node->key = realloc(node->key,
				node->sb.items * sizeof(*node->key)));
	assert(node->value = realloc(node->value,
				node->sb.items * sizeof(*node->value)));

	for (size_t i = 0; i != node->sb.items; ++i) {
		struct __myfs_ctree_item *__item =
					(struct __myfs_ctree_item *)pos;
		struct myfs_ctree_item item;

		myfs_ctree_item2mem(&item, __item);
		pos += sizeof(*__item);

		node->key[i].size = item.key_size;
		node->key[i].data = pos;
		pos += item.key_size;

		node->value[i].size = item.value_size;
		node->value[i].data = pos;
		pos += item.value_size;
	}
	return 0;
}

static void myfs_node_reset(struct myfs_ctree_node *node)
{
	memset(&node->ptr, 0, sizeof(node->ptr));
}

static void myfs_node_release(struct myfs_ctree_node *node)
{
	free(node->buf);
	free(node->key);
	free(node->value);
	memset(node, 0, sizeof(*node));
}


void myfs_ctree_it_setup(struct myfs_ctree_it *it,
			const struct myfs_ctree_sb *sb)
{
	memset(it, 0, sizeof(*it));
	it->sb = *sb;
}

void myfs_ctree_it_release(struct myfs_ctree_it *it)
{
	for (size_t i = 0; i != MYFS_MAX_CTREE_HIGHT; ++i)
		myfs_node_release(&it->node[i]);
	memset(it, 0, sizeof(*it));
}

int myfs_ctree_it_valid(const struct myfs_ctree_it *it)
{
	return it->pos[0] < it->node[0].sb.items;
}

static int myfs_ctree_it_advance(struct myfs *myfs, struct myfs_ctree_it *it)
{
	const size_t hight = it->sb.hight;

	size_t top = 0;

	if (++it->pos[0] < it->node[0].sb.items)
		return 0;

	for (size_t i = 1; i < hight; ++i) {
		if (it->pos[i] < it->node[i].sb.items - 1) {
			top = i;
			break;
		}
	}

	if (!top)
		return 0;

	for (size_t i = 0; i != top; ++i) {
		myfs_node_reset(&it->node[i]);
		it->pos[i] = 0;
	}

	++it->pos[top];
	for (size_t i = top; i; --i) {
		const size_t pos = it->pos[i];
		const struct myfs_value value = it->node[i].value[pos];
		const struct __myfs_ptr *__ptr = value.data;
		struct myfs_ptr ptr;

		assert(value.size == sizeof(*__ptr));
		myfs_ptr2mem(&ptr, __ptr);

		const int err = myfs_node_read(myfs, &it->node[i - 1], &ptr);

		if (err)
			return err;
	}
	return 0;
}

int myfs_ctree_it_next(struct myfs *myfs, struct myfs_ctree_it *it)
{
	if (!myfs_ctree_it_valid(it))
		return 0;

	const int err = myfs_ctree_it_advance(myfs, it);

	if (err)
		return err;

	if (!myfs_ctree_it_valid(it)) {
		memset(&it->key, 0, sizeof(it->key));
		memset(&it->value, 0, sizeof(it->value));
		return 0;
	}

	it->key = it->node[0].key[it->pos[0]];
	it->value = it->node[0].value[it->pos[0]];
	return 0;
}


static size_t myfs_node_lookup(const struct myfs_ctree_node *node,
			struct myfs_query *query)
{
	size_t l = 0, r = node->sb.items;

	while (l < r) {
		const size_t m = l + (r - l) / 2;

		if (query->cmp(query, &node->key[m]) >= 0)
			r = m;
		else
			l = m + 1;
	}
	return l;
}

int myfs_ctree_it_find(struct myfs *myfs, struct myfs_ctree_it *it,
			struct myfs_query *query)
{
	const size_t hight = it->sb.hight;
	struct myfs_ptr ptr = it->sb.root;

	if (!hight)
		return 0;

	for (size_t i = hight; i > 1; --i) {
		struct __myfs_ptr __ptr;
		struct myfs_ctree_node *node = &it->node[i - 1];
		const int err = myfs_node_read(myfs, node, &ptr);

		if (err)
			return err;

		#define MIN(a, b) ((a < b) ? a : b)
		const size_t pos = MIN(myfs_node_lookup(node, query),
					node->sb.items - 1);
		#undef MIN

		it->pos[i - 1] = pos;
		assert(node->value[pos].size == sizeof(__ptr));
		memcpy(&__ptr, node->value[pos].data, sizeof(__ptr));
		myfs_ptr2mem(&ptr, &__ptr);
	}

	struct myfs_ctree_node *node = &it->node[0];
	const int err = myfs_node_read(myfs, node, &ptr);

	if (err)
		return err;

	it->pos[0] = myfs_node_lookup(node, query);
	if (myfs_ctree_it_valid(it)) {
		it->key = it->node[0].key[it->pos[0]];
		it->value = it->node[0].value[it->pos[0]];
	} else {
		memset(&it->key, 0, sizeof(it->key));
		memset(&it->value, 0, sizeof(it->value));
	}
	return 0;
}


static int myfs_ctree_reset_cmp(struct myfs_query *query,
			const struct myfs_key *key)
{
	(void) query;
	(void) key;
	return 1;
}

int myfs_ctree_it_reset(struct myfs *myfs, struct myfs_ctree_it *it)
{
	struct myfs_query q = { &myfs_ctree_reset_cmp, NULL };

	return myfs_ctree_it_find(myfs, it, &q);
}


int myfs_ctree_lookup(struct myfs *myfs, const struct myfs_ctree_sb *sb,
			struct myfs_query *query)
{
	struct myfs_ctree_it it;
	int err;

	myfs_ctree_it_setup(&it, sb);
	if ((err = myfs_ctree_it_find(myfs, &it, query)))
		goto out;

	if (!myfs_ctree_it_valid(&it))
		goto out;

	err = 0;
	if (!query->cmp(query, &it.key))
		err = query->emit(query, &it.key, &it.value);

out:
	myfs_ctree_it_release(&it);
	return err;
}

int myfs_ctree_range(struct myfs *myfs, const struct myfs_ctree_sb *sb,
			struct myfs_query *query)
{
	struct myfs_ctree_it it;
	int err;

	myfs_ctree_it_setup(&it, sb);
	if ((err = myfs_ctree_it_find(myfs, &it, query)))
		goto out;

	while (!err && myfs_ctree_it_valid(&it)) {
		if (query->cmp(query, &it.key))
			break;

		err = query->emit(query, &it.key, &it.value);
		if (err)
			break;

		err = myfs_ctree_it_next(myfs, &it);
	}

out:
	myfs_ctree_it_release(&it);
	return err;
}
