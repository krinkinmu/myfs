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
#include <lsm/skip.h>

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>


static struct myfs_skip_node *myfs_skip_node_create(size_t hight,
			const struct myfs_key *key,
			const struct myfs_value *value)
{
	const size_t size = sizeof(struct myfs_skip_node) +
				(hight - 1) * sizeof(struct myfs_mtree_node *) +
				key->size + value->size;
	struct myfs_skip_node *node = malloc(size);
	char *buf;

	assert(node);
	memset(node, 0, size);
	buf = (char *)(node->next + hight);

	if (key->size)
		memcpy(buf, key->data, key->size);
	node->key.data = buf;
	node->key.size = key->size;
	buf += key->size;

	if (value->size)
		memcpy(buf, value->data, value->size);
	node->value.data = buf;
	node->value.size = value->size;

	return node;
}

static void myfs_skip_node_destroy(struct myfs_skip_node *node)
{
	free(node);
}


static int mtree_skip_insert(struct myfs_mtree *mtree,
			const struct myfs_key *key,
			const struct myfs_value *value)
{
	struct myfs_skiplist *skip = (struct myfs_skiplist *)mtree;

	return myfs_skip_insert(skip, key, value);
}

static int mtree_skip_lookup(struct myfs_mtree *mtree, struct myfs_query *query)
{
	struct myfs_skiplist *skip = (struct myfs_skiplist *)mtree;

	return myfs_skip_lookup(skip, query);
}

static int mtree_skip_range(struct myfs_mtree *mtree, struct myfs_query *query)
{
	struct myfs_skiplist *skip = (struct myfs_skiplist *)mtree;

	return myfs_skip_range(skip, query);
}

static int mtree_skip_scan(struct myfs_mtree *mtree, struct myfs_query *query)
{
	struct myfs_skiplist *skip = (struct myfs_skiplist *)mtree;

	return myfs_skip_scan(skip, query);
}

static size_t mtree_skip_size(const struct myfs_mtree *mtree)
{
	const struct myfs_skiplist *skip = (const struct myfs_skiplist *)mtree;

	return myfs_skip_size(skip);
}


void myfs_skiplist_setup(struct myfs_skiplist *tree, myfs_cmp_t cmp)
{
	const struct myfs_key key = { 0, 0 };
	const struct myfs_value value = { 0, 0 };

	memset(tree, 0, sizeof(*tree));
	tree->head = myfs_skip_node_create(MYFS_MAX_MTREE_HIGHT,
				&key, &value);
	tree->cmp = cmp;

	tree->mtree.insert = &mtree_skip_insert;
	tree->mtree.lookup = &mtree_skip_lookup;
	tree->mtree.range = &mtree_skip_range;
	tree->mtree.scan = &mtree_skip_scan;
	tree->mtree.size = &mtree_skip_size;
}

void myfs_skiplist_release(struct myfs_skiplist *tree)
{
	struct myfs_skip_node *ptr = tree->head;

	while (ptr) {
		struct myfs_skip_node *node = ptr;

		ptr = atomic_load_explicit(node->next, memory_order_relaxed);
		myfs_skip_node_destroy(node);
	}
	memset(tree, 0, sizeof(*tree));
}


static size_t myfs_skip_node_hight(size_t maxh)
{
	for (size_t h = 0; h != maxh; ++h) {
		if (rand() % 2)
			return h + 1;
	}
	return maxh;
}

int myfs_skip_insert(struct myfs_skiplist *tree, const struct myfs_key *key,
			const struct myfs_value *value)
{
	const size_t size = atomic_fetch_add_explicit(&tree->size, 1,
				memory_order_relaxed);
	const size_t hight = myfs_skip_node_hight(MYFS_MAX_MTREE_HIGHT);
	struct myfs_skip_node *node = myfs_skip_node_create(hight,
				key, value);
	struct myfs_skip_node *ptr = tree->head;
	struct myfs_skip_node *tower[MYFS_MAX_MTREE_HIGHT];

	node->seq = size;

	for (size_t h = MYFS_MAX_MTREE_HIGHT; h; --h) {
		while (1) {
			struct myfs_skip_node *n = atomic_load_explicit(
						&ptr->next[h - 1],
						memory_order_consume);

			if (!n) {
				tower[h - 1] = ptr;
				break;
			}

			const int res = tree->cmp(key, &n->key);

			if ((res > 0) || (!res && node->seq < n->seq)) {
				ptr = n;
				continue; 
			}

			tower[h - 1] = ptr;
			break;
		}
	}

	for (size_t h = 0; h != hight; ++h) {
		while (1) {
			struct myfs_skip_node *n = atomic_load_explicit(
						&tower[h]->next[h],
						memory_order_consume);

			atomic_store_explicit(&node->next[h], n,
						memory_order_relaxed);

			if (!n) {
				if (atomic_compare_exchange_strong_explicit(
							&tower[h]->next[h],
							&node->next[h], node,
							memory_order_release,
							memory_order_relaxed))
					break;
				else
					continue;
			}

			const int res = tree->cmp(key, &n->key);

			if ((res > 0) || (!res && node->seq < n->seq)) {
				tower[h] = n;
				continue; 
			}

			if (atomic_compare_exchange_strong_explicit(
						&tower[h]->next[h],
						&node->next[h], node,
						memory_order_release,
						memory_order_relaxed))
				break;
			else
				continue;
		}
	}
	return 0;
}


static struct myfs_skip_node *myfs_skip_query(struct myfs_skiplist *tree,
			struct myfs_query *query)
{
	struct myfs_skip_node *ptr = tree->head;
	
	for (size_t h = MYFS_MAX_MTREE_HIGHT; h; --h) {
		while (1) {
			struct myfs_skip_node *n = atomic_load_explicit(
						&ptr->next[h - 1],
						memory_order_consume);

			if (!n)
				break;

			if (query->cmp(query, &n->key) >= 0)
				break;
			ptr = n;
		}
	}

	ptr = atomic_load_explicit(&ptr->next[0], memory_order_consume);
	while (ptr && query->cmp(query, &ptr->key) < 0)
		ptr = atomic_load_explicit(&ptr->next[0], memory_order_consume);
	return ptr;
}


int myfs_skip_lookup(struct myfs_skiplist *skip, struct myfs_query *query)
{
	struct myfs_skip_node *node = myfs_skip_query(skip, query);

	if (!node)
		return 0;

	if (query->cmp(query, &node->key))
		return 0;
	return query->emit(query, &node->key, &node->value);
}

int myfs_skip_range(struct myfs_skiplist *skip, struct myfs_query *query)
{
	struct myfs_skip_node *node = myfs_skip_query(skip, query);
	int err = 0;

	while (node && !query->cmp(query, &node->key)) {
		struct myfs_skip_node *n = node;

		err = query->emit(query, &node->key, &node->value);
		if (err)
			break;

		do {
			n = atomic_load_explicit(&n->next[0],
						memory_order_consume);
		} while (n && !skip->cmp(&n->key, &node->key));
		node = n;
	}
	return err;
}

int myfs_skip_scan(struct myfs_skiplist *skip, struct myfs_query *query)
{
	struct myfs_skip_node *node = atomic_load_explicit(&skip->head->next[0],
				memory_order_consume);
	int err = 0;

	while (node) {
		struct myfs_skip_node *n = node;

		if (!query->cmp(query, &node->key)) {
			err = query->emit(query, &node->key, &node->value);
			if (err)
				break;
		}

		do {
			n = atomic_load_explicit(&n->next[0],
						memory_order_consume);
		} while (n && !skip->cmp(&n->key, &node->key));
		node = n;
	}
	return err;
}

size_t myfs_skip_size(const struct myfs_skiplist *skip)
{
	return atomic_load_explicit(&skip->size, memory_order_relaxed);
}
