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
#include <lsm/lsm.h>
#include <lsm/ctree.h>
#include <lsm/skip.h>
#include <myfs.h>

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>



const struct myfs_lsm_policy myfs_lsm_default_policy = {
	.create = &myfs_lsm_create_default,
	.destroy = &myfs_lsm_destroy_default,
	.flush = &myfs_lsm_flush_default,
	.merge = &myfs_lsm_merge_default,
	.insert = &myfs_lsm_insert_default,
	.lookup = &myfs_lsm_lookup_default,
	.range = &myfs_lsm_range_default
};



struct myfs_item {
	size_t item_offs;
	size_t key_size;
	size_t value_size;
};


struct myfs_items {
	void *buf;
	size_t buf_size, buf_cap;

	struct myfs_item *item;
	size_t size, cap;
};


static void myfs_items_setup(struct myfs_items *items)
{
	memset(items, 0, sizeof(*items));
}

static void myfs_items_release(struct myfs_items *items)
{
	free(items->buf);
	free(items->item);
}

static void myfs_items_get(const struct myfs_items *items, size_t i,
			struct myfs_key *key, struct myfs_value *value)
{
	assert(i < items->size);

	const struct myfs_item *item = &items->item[i];
	char *ptr = (char *)items->buf + item->item_offs;

	key->data = ptr;
	key->size = item->key_size;
	value->data = ptr + item->key_size;
	value->size = item->value_size;
}

static void myfs_items_append(struct myfs_items *items,
			const struct myfs_key *key,
			const struct myfs_value *value)
{
	const size_t size = key->size + value->size;

	if (items->buf_size + size > items->buf_cap) {
		#define MAX(a, b) (((a) < (b)) ? (b) : (a))
		const size_t cap = items->buf_cap ? items->buf_cap * 2 : 4096;
		const size_t alloc = MAX(cap, size);
		#undef MAX

		assert(items->buf = realloc(items->buf, alloc));
		items->buf_cap = alloc;
	}

	if (items->size == items->cap) {
		const size_t cap = items->cap ? items->cap * 2 : 256;
		const size_t alloc = cap * sizeof(*items->item);

		assert(items->item = realloc(items->item, alloc));
		items->cap = cap;
	}

	struct myfs_item *item = &items->item[items->size++];
	char *ptr = (char *)items->buf + items->buf_size;

	memcpy(ptr, key->data, key->size);
	memcpy(ptr + key->size, value->data, value->size);
	item->item_offs = items->buf_size;
	item->key_size = key->size;
	item->value_size = value->size;
	items->buf_size += size;
}



struct myfs_arr_input_it {
	struct myfs_input_it it;
	struct myfs_items *items;
	size_t pos;
};


static int myfs_arr_input_valid(struct myfs_input_it *it)
{
	struct myfs_arr_input_it *input = (struct myfs_arr_input_it *)it;

	return input->pos < input->items->size;
}

static int myfs_arr_input_next(struct myfs_input_it *it)
{
	struct myfs_arr_input_it *input = (struct myfs_arr_input_it *)it;

	assert(input->pos < input->items->size);
	++input->pos;
	return 0;
}

static int myfs_arr_input_entry(struct myfs_input_it *it, struct myfs_key *key,
			struct myfs_value *value)
{
	struct myfs_arr_input_it *input = (struct myfs_arr_input_it *)it;

	assert(input->pos < input->items->size);
	myfs_items_get(input->items, input->pos, key, value);
	return 0;
}

void myfs_arr_input_setup(struct myfs_arr_input_it *it,
			struct myfs_items *items)
{
	it->it.valid = &myfs_arr_input_valid;
	it->it.next = &myfs_arr_input_next;
	it->it.entry = &myfs_arr_input_entry;
	it->items = items;
	it->pos = 0;
}



struct myfs_ctree_input_it {
	struct myfs_input_it it;
	struct myfs_ctree_it *cit;
	struct myfs_query *query;
	struct myfs *myfs;
};


static int myfs_ctree_input_valid(struct myfs_input_it *it)
{
	struct myfs_ctree_input_it *input = (struct myfs_ctree_input_it *)it;
	struct myfs_ctree_it *cit = input->cit;
	struct myfs_query *q = input->query;

	if (!myfs_ctree_it_valid(cit))
		return 0;

	if (q && q->cmp(q, &cit->key))
		return 0;
	return 1;
}

static int myfs_ctree_input_next(struct myfs_input_it *it)
{
	struct myfs_ctree_input_it *input = (struct myfs_ctree_input_it *)it;
	struct myfs_ctree_it *cit = input->cit;
	struct myfs *myfs = input->myfs;

	assert(myfs_ctree_input_valid(it));
	return myfs_ctree_it_next(myfs, cit);
}

static int myfs_ctree_input_entry(struct myfs_input_it *it,
			struct myfs_key *key, struct myfs_value *value)
{
	struct myfs_ctree_input_it *input = (struct myfs_ctree_input_it *)it;
	struct myfs_ctree_it *cit = input->cit;

	assert(myfs_ctree_input_valid(it));
	*key = cit->key;
	*value = cit->value;
	return 0;
}

void myfs_ctree_input_setup(struct myfs_ctree_input_it *it,
			struct myfs *myfs, struct myfs_ctree_it *cit,
			struct myfs_query *query)
{
	it->it.valid = &myfs_ctree_input_valid;
	it->it.next = &myfs_ctree_input_next;
	it->it.entry = &myfs_ctree_input_entry;
	it->cit = cit;
	it->query = query;
	it->myfs = myfs;
}



struct myfs_query_output_it {
	struct myfs_output_it it;
	struct myfs_query *query;
	const struct myfs_key_ops *ops;
};


static int myfs_query_output_emit(struct myfs_output_it *it,
			const struct myfs_key *key,
			const struct myfs_value *value)
{
	struct myfs_query_output_it *out = (struct myfs_query_output_it *)it;

	if (out->ops && out->ops->deleted(key, value))
		return 0;
	return out->query->emit(out->query, key, value);
}

static void myfs_query_output_setup(struct myfs_query_output_it *it,
			struct myfs_query *query,
			const struct myfs_key_ops *kops)
{
	it->it.emit = &myfs_query_output_emit;
	it->query = query;
	it->ops = kops;
}



struct myfs_lookup_query {
	struct myfs_query proxy;
	struct myfs_query *orig;
	int found;
};


static int myfs_lookup_cmp(struct myfs_query *p, const struct myfs_key *key)
{
	struct myfs_lookup_query *proxy = (struct myfs_lookup_query *)p;

	return proxy->orig->cmp(proxy->orig, key);
}

static int myfs_lookup_emit(struct myfs_query *p, const struct myfs_key *key,
			const struct myfs_value *value)
{
	struct myfs_lookup_query *proxy = (struct myfs_lookup_query *)p;

	proxy->found = 1;
	return proxy->orig->emit(proxy->orig, key, value);
}

int myfs_lsm_lookup_default(struct myfs_lsm *lsm, struct myfs_query *query)
{
	struct myfs_lookup_query proxy = {
		{ &myfs_lookup_cmp, &myfs_lookup_emit },
		query, 0
	};
	struct myfs *myfs = lsm->myfs;
	int err = 0;

	assert(!pthread_rwlock_rdlock(&lsm->mtlock));
	err = lsm->c0->lookup(lsm->c0, &proxy.proxy);
	if (!proxy.found && !err && lsm->c1)
		err = lsm->c1->lookup(lsm->c1, &proxy.proxy);
	assert(!pthread_rwlock_unlock(&lsm->mtlock));

	if (proxy.found || err)
		return err;

	for (int i = 0; i != MYFS_MAX_TREES; ++i) {
		struct myfs_ctree_sb sb;

		assert(!pthread_rwlock_rdlock(&lsm->sblock));
		sb = lsm->sb.tree[i];
		err = myfs_ctree_lookup(myfs, &sb, &proxy.proxy);
		assert(!pthread_rwlock_unlock(&lsm->sblock));

		if (err || proxy.found)
			break;
	}
	return err;
}



int myfs_lsm_insert_default(struct myfs_lsm *lsm, const struct myfs_key *key,
			const struct myfs_value *value)
{
	int err;

	assert(!pthread_rwlock_rdlock(&lsm->mtlock));
	err = lsm->c0->insert(lsm->c0, key, value);
	assert(!pthread_rwlock_unlock(&lsm->mtlock));

	return err;
}



struct myfs_range_query {
	struct myfs_query proxy;
	struct myfs_query *orig;
	struct myfs_items items;
};


static int myfs_range_cmp(struct myfs_query *p, const struct myfs_key *key)
{
	struct myfs_range_query *proxy = (struct myfs_range_query *)p;

	return proxy->orig->cmp(proxy->orig, key);
}

static int myfs_range_emit(struct myfs_query *p, const struct myfs_key *key,
			const struct myfs_value *value)
{
	struct myfs_range_query *proxy = (struct myfs_range_query *)p;

	myfs_items_append(&proxy->items, key, value);
	return 0;
}

static void myfs_range_setup(struct myfs_range_query *query,
			struct myfs_query *orig)
{
	memset(query, 0, sizeof(*query));
	query->proxy.cmp = &myfs_range_cmp;
	query->proxy.emit = &myfs_range_emit;
	query->orig = orig;
	myfs_items_setup(&query->items);
}

static void myfs_range_release(struct myfs_range_query *query)
{
	myfs_items_release(&query->items);
}

static int myfs_merge_ranges(struct myfs_lsm *lsm,
			struct myfs_output_it *output,
			struct myfs_input_it **input, size_t size)
{
	const myfs_cmp_t cmp = lsm->key_ops->cmp;

	while (1) {
		struct myfs_key key = { 0, NULL };
		struct myfs_value value = { 0, NULL };

		for (size_t i = 0; i != size; ++i) {
			if (!input[i]->valid(input[i]))
				continue;

			struct myfs_key k;
			struct myfs_value v;

			input[i]->entry(input[i], &k, &v);
			if (!key.data || cmp(&k, &key) < 0) {
				key = k;
				value = v;
			}
		}

		if (!key.data)
			break;

		int err = output->emit(output, &key, &value);

		if (err)
			return err;

		for (size_t i = 0; i != size; ++i) {
			if (!input[i]->valid(input[i]))
				continue;

			struct myfs_key k;
			struct myfs_value v;

			input[i]->entry(input[i], &k, &v);
			if (!cmp(&key, &k)) {
				err = input[i]->next(input[i]);
				if (err)
					return err;
			}
		}
	}

	return 0;
}

int myfs_lsm_range_default(struct myfs_lsm *lsm, struct myfs_query *query)
{
	#define SIZE (MYFS_MAX_TREES + 2)
	struct myfs_arr_input_it mit[2];
	struct myfs_ctree_input_it dit[MYFS_MAX_TREES];
	struct myfs_input_it *input[SIZE];

	struct myfs_ctree_it cit[MYFS_MAX_TREES];
	struct myfs_range_query proxy[2];

	struct myfs *myfs = lsm->myfs;
	int err = 0;

	myfs_range_setup(&proxy[0], query);
	myfs_range_setup(&proxy[1], query);

	assert(!pthread_rwlock_rdlock(&lsm->mtlock));
	err = lsm->c0->range(lsm->c0, &proxy[0].proxy);
	if (!err && lsm->c1)
		err = lsm->c1->range(lsm->c1, &proxy[1].proxy);
	assert(!pthread_rwlock_unlock(&lsm->mtlock));

	assert(!pthread_rwlock_rdlock(&lsm->sblock));
	for (int i = 0; i != MYFS_MAX_TREES; ++i)
		myfs_ctree_it_setup(&cit[i], &lsm->sb.tree[i]);
	
	for (int i = 0; !err && i != MYFS_MAX_TREES; ++i)
		err = myfs_ctree_it_find(myfs, &cit[i], query);

	if (!err) {
		struct myfs_query_output_it output;

		myfs_arr_input_setup(&mit[0], &proxy[0].items);
		myfs_arr_input_setup(&mit[1], &proxy[1].items);
		input[0] = &mit[0].it;
		input[1] = &mit[1].it;
		for (int i = 0; i != MYFS_MAX_TREES; ++i) {
			myfs_ctree_input_setup(&dit[i], myfs, &cit[i], query);
			input[2 + i] = &dit[i].it;
		}

		myfs_query_output_setup(&output, query, lsm->key_ops);
		err = myfs_merge_ranges(lsm, &output.it, input, SIZE);
	}

	for (int i = 0; i != MYFS_MAX_TREES; ++i)
		myfs_ctree_it_release(&cit[i]);
	assert(!pthread_rwlock_unlock(&lsm->sblock));

	myfs_range_release(&proxy[1]);
	myfs_range_release(&proxy[0]);
	#undef SIZE
	return err;
}



struct myfs_flush_query {
	struct myfs_query query;
	struct myfs_items items;
};


static int myfs_flush_cmp(struct myfs_query *query, const struct myfs_key *key)
{
	(void) query;
	(void) key;
	return 0;
}

static int myfs_flush_emit(struct myfs_query *query, const struct myfs_key *key,
			const struct myfs_value *value)
{
	struct myfs_flush_query *flush = (struct myfs_flush_query *)query;

	myfs_items_append(&flush->items, key, value);
	return 0;
}

static void myfs_flush_setup(struct myfs_flush_query *flush)
{
	flush->query.cmp = &myfs_flush_cmp;
	flush->query.emit = &myfs_flush_emit;
	myfs_items_setup(&flush->items);
}

static void myfs_flush_release(struct myfs_flush_query *flush)
{
	myfs_items_release(&flush->items);
}


static int myfs_lsm_append(struct myfs_lsm *lsm,
			struct myfs_ctree_builder *build,
			const struct myfs_key *key,
			const struct myfs_value *value)
{
	struct myfs *myfs = lsm->myfs;

	if (lsm->size <= 1 && lsm->key_ops->deleted(key, value))
		return 0;

	return myfs_builder_append(myfs, build, key, value);
}

static int __myfs_lsm_flush_default(struct myfs_lsm *lsm,
			const struct myfs_ctree_sb *sb,
			const struct myfs_items *items,
			struct myfs_ctree_builder *build)
{
	struct myfs_ctree_it it;
	struct myfs_key key;
	struct myfs_value value;

	struct myfs *myfs = lsm->myfs;
	size_t pos = 0;
	int err = 0;

	myfs_ctree_it_setup(&it, sb);
	err = myfs_ctree_it_reset(myfs, &it);
	if (err) {
		myfs_ctree_it_release(&it);
		return err;
	}

	while (myfs_ctree_it_valid(&it)) {
		const myfs_cmp_t cmp = lsm->key_ops->cmp;

		while (pos != items->size) {
			myfs_items_get(items, pos, &key, &value);
			if (cmp(&key, &it.key) >= 0)
				break;

			err = myfs_lsm_append(lsm, build, &key, &value);
			if (err)
				break;

			++pos;
		}

		if (err)
			break;

		if (pos != items->size && !cmp(&key, &it.key)) {
			err = myfs_lsm_append(lsm, build, &key, &value);
			if (err)
				break;
			++pos;
		} else {
			err = myfs_lsm_append(lsm, build, &it.key, &it.value);
			if (err)
				break;
		}

		err = myfs_ctree_it_next(myfs, &it);
		if (err < 0)
			break;
	}

	while (!err && pos != items->size) {
		myfs_items_get(items, pos++, &key, &value);
		err = myfs_lsm_append(lsm, build, &key, &value);
	}
	myfs_ctree_it_release(&it);
	return err;
}

int myfs_lsm_flush_default(struct myfs_lsm *lsm, struct myfs_mtree *new,
			const struct myfs_ctree_sb *old,
			struct myfs_ctree_sb *res)
{
	struct myfs_ctree_builder build;
	struct myfs_flush_query flush;

	struct myfs *myfs = lsm->myfs;
	int err = 0;

	myfs_flush_setup(&flush);
	err = new->scan(new, &flush.query);
	if (err) {
		myfs_flush_release(&flush);
		return err;
	}

	myfs_builder_setup(&build);
	if (!(err = __myfs_lsm_flush_default(lsm, old, &flush.items, &build)))
		if (!(err = myfs_builder_finish(myfs, &build)))
			*res = build.sb;
	myfs_builder_release(&build);
	myfs_flush_release(&flush);
	return err;
}



static int ___myfs_lsm_merge_default(struct myfs_lsm *lsm,
			struct myfs_ctree_it *it,
			struct myfs_ctree_builder *build)
{
	const myfs_cmp_t cmp = lsm->key_ops->cmp;
	struct myfs *myfs = lsm->myfs;
	int valid[2];
	int err = 0;

	valid[0] = myfs_ctree_it_valid(&it[0]);
	valid[1] = myfs_ctree_it_valid(&it[1]);

	while (err >= 0 && valid[0] && valid[1]) {
		const int res = cmp(&it[0].key, &it[1].key);
		const int i = res <= 0 ? 0 : 1;

		err = myfs_lsm_append(lsm, build, &it[i].key, &it[i].value);
		if (err)
			return err;

		if (res <= 0) {
			err = myfs_ctree_it_next(myfs, &it[0]);
			if (err < 0)
				return err;
			valid[0] = myfs_ctree_it_valid(&it[0]);
		}

		if (res >= 0) {
			err = myfs_ctree_it_next(myfs, &it[1]);
			if (err < 0)
				return err;
			valid[1] = myfs_ctree_it_valid(&it[1]);
		}
	}

	for (int i = 0; err >= 0 && i != 2; ++i, err = 0) {
		while (err >= 0 && valid[i]) {
			err = myfs_lsm_append(lsm, build, &it[i].key,
						&it[i].value);
			if (err)
				return err;

			err = myfs_ctree_it_next(myfs, &it[i]);
			if (err < 0)
				return err;
			valid[i] = myfs_ctree_it_valid(&it[i]);
		}
	}
	return err;
}

static int __myfs_lsm_merge_default(struct myfs_lsm *lsm,
			const struct myfs_ctree_sb *sb,
			struct myfs_ctree_builder *build)
{
	struct myfs *myfs = lsm->myfs;
	struct myfs_ctree_it it[2];
	int err = 0;

	for (int i = 0; i != 2; ++i)
		myfs_ctree_it_setup(&it[i], &sb[i]);

	for (int i = 0; i != 2; ++i)
		if ((err = myfs_ctree_it_reset(myfs, &it[i])))
			break;

	if (!err)
		err = ___myfs_lsm_merge_default(lsm, it, build);

	for (int i = 0; i != 2; ++i)
		myfs_ctree_it_release(&it[i]);
	return err;
}

int myfs_lsm_merge_default(struct myfs_lsm *lsm,
			const struct myfs_ctree_sb *new,
			const struct myfs_ctree_sb *old,
			struct myfs_ctree_sb *res)
{
	struct myfs *myfs = lsm->myfs;
	struct myfs_ctree_builder build;
	struct myfs_ctree_sb sb[2];
	int err = 0;

	sb[0] = *new;
	sb[1] = *old;

	myfs_builder_setup(&build);
	if (!(err = __myfs_lsm_merge_default(lsm, sb, &build)))
		if (!(err = myfs_builder_finish(myfs, &build)))
			*res = build.sb;
	myfs_builder_release(&build);
	return err;
}



void myfs_lsm_setup(struct myfs_lsm *lsm, struct myfs *myfs,
			const struct myfs_lsm_policy *lops,
			const struct myfs_key_ops *kops,
			const struct myfs_lsm_sb *sb)
{
	memset(lsm, 0, sizeof(*lsm));

	lsm->sb = *sb;
	lsm->myfs = myfs;
	lsm->policy = lops;
	lsm->key_ops = kops;

	assert(!pthread_rwlock_init(&lsm->sblock, NULL));
	assert(!pthread_rwlock_init(&lsm->mtlock, NULL));
	assert(!pthread_mutex_init(&lsm->mtx, NULL));
	assert(!pthread_cond_init(&lsm->cv, NULL));
	assert(lsm->c0 = lops->create(lsm));

	for (size_t i = MYFS_MAX_TREES; i; --i) {
		if (sb->tree[i - 1].hight) {
			lsm->size = i;
			break;
		}
	}
}

void myfs_lsm_release(struct myfs_lsm *lsm)
{
	lsm->policy->destroy(lsm, lsm->c0);
	if (lsm->c1)
		lsm->policy->destroy(lsm, lsm->c1);
	assert(!pthread_rwlock_destroy(&lsm->sblock));
	assert(!pthread_rwlock_destroy(&lsm->mtlock));
	assert(!pthread_mutex_destroy(&lsm->mtx));
	assert(!pthread_cond_destroy(&lsm->cv));
	memset(lsm, 0, sizeof(*lsm));
}

void myfs_lsm_get_root(struct myfs_lsm_sb *sb, struct myfs_lsm *lsm)
{
	assert(!pthread_rwlock_rdlock(&lsm->sblock));
	*sb = lsm->sb;
	assert(!pthread_rwlock_unlock(&lsm->sblock));
}



int myfs_lsm_insert(struct myfs_lsm *lsm, const struct myfs_key *key,
			const struct myfs_value *value)
{
	return lsm->policy->insert(lsm, key, value);
}

int myfs_lsm_lookup(struct myfs_lsm *lsm, struct myfs_query *query)
{
	return lsm->policy->lookup(lsm, query);
}

int myfs_lsm_range(struct myfs_lsm *lsm, struct myfs_query *query)
{
	return lsm->policy->range(lsm, query);
}



static void myfs_lsm_start_merge(struct myfs_lsm *lsm, int from, int to)
{
	assert(!pthread_mutex_lock(&lsm->mtx));
	while (1) {
		int wait = 0;

		for (int i = from; i <= to; ++i)
			wait = wait || lsm->merge[i];

		if (!wait)
			break;
		pthread_cond_wait(&lsm->cv, &lsm->mtx);
	}

	for (int i = from; i <= to; ++i)
		lsm->merge[i] = 1;

	assert(!pthread_mutex_unlock(&lsm->mtx));
}

static void myfs_lsm_finish_merge(struct myfs_lsm *lsm, int from, int to)
{
	assert(!pthread_mutex_lock(&lsm->mtx));
	for (int i = from; i <= to; ++i)
		lsm->merge[i] = 0;
	assert(!pthread_cond_broadcast(&lsm->cv));
	assert(!pthread_mutex_unlock(&lsm->mtx));
}


static int __myfs_lsm_merge(struct myfs_lsm *lsm, size_t i)
{
	struct myfs_ctree_sb from[2];
	struct myfs_ctree_sb sb;

	assert(!pthread_rwlock_rdlock(&lsm->sblock));
	from[0] = lsm->sb.tree[i];
	from[1] = lsm->sb.tree[i + 1];
	assert(!pthread_rwlock_unlock(&lsm->sblock));

	if (!from[0].hight)
		return 0;

	if (from[1].hight) {
		const int err = lsm->policy->merge(lsm,
					&from[0], &from[1], &sb);

		if (err)
			return err;
	} else {
		sb = from[0];
	}

	assert(!pthread_rwlock_wrlock(&lsm->sblock));
	lsm->sb.tree[i + 1] = sb;
	memset(&lsm->sb.tree[i], 0, sizeof(lsm->sb.tree[i]));
	if (i + 2 > lsm->size)
		lsm->size = i + 2;
	assert(!pthread_rwlock_unlock(&lsm->sblock));

	return 0;
}

int myfs_lsm_merge(struct myfs_lsm *lsm, size_t i)
{
	int err;

	if (i >= MYFS_MAX_TREES - 1)
		return 0;

	myfs_lsm_start_merge(lsm, i, i + 1);
	err = __myfs_lsm_merge(lsm, i);
	myfs_lsm_finish_merge(lsm, i, i + 1);
	return err;
}

int myfs_lsm_need_flush(struct myfs_lsm *lsm)
{
	size_t size;

	assert(!pthread_rwlock_rdlock(&lsm->mtlock));
	size = lsm->c0->size(lsm->c0);
	assert(!pthread_rwlock_unlock(&lsm->mtlock));
	return size >= MYFS_MTREE_SIZE; 
}

int myfs_lsm_need_merge(struct myfs_lsm *lsm, size_t i)
{
	if (i > MYFS_MAX_TREES - 1)
		return 0;

	struct myfs *myfs = lsm->myfs;
	struct myfs_lsm_sb sb;
	uint32_t max_size = MYFS_C0_SIZE;

	for (size_t j = 0; j != i; ++j)
		max_size *= MYFS_CX_MULT;
	myfs_lsm_get_root(&sb, lsm);
	return sb.tree[i].size * myfs->page_size >= max_size;
}

static int __myfs_lsm_flush_start(struct myfs_lsm *lsm)
{
	assert(!pthread_rwlock_wrlock(&lsm->mtlock));
	if (lsm->c1) {
		assert(!pthread_rwlock_unlock(&lsm->mtlock));
		return -EBUSY;
	}

	lsm->c1 = lsm->c0;
	assert(lsm->c0 = lsm->policy->create(lsm));
	assert(!pthread_rwlock_unlock(&lsm->mtlock));
	return 0;
}

static int __myfs_lsm_flush_finish(struct myfs_lsm *lsm)
{
	struct myfs_ctree_sb old, res;
	int err = 0;

	assert(!pthread_rwlock_rdlock(&lsm->sblock));
	old = lsm->sb.tree[0];
	assert(!pthread_rwlock_unlock(&lsm->sblock));

	if (lsm->c1->size(lsm->c1))
		err = lsm->policy->flush(lsm, lsm->c1, &old, &res);
	else
		res = old;

	assert(!pthread_rwlock_wrlock(&lsm->sblock));
	if (!err) {
		struct myfs_mtree *c1;

		lsm->sb.tree[0] = res;
		if (res.hight && !lsm->size)
			lsm->size = 1;

		assert(!pthread_rwlock_wrlock(&lsm->mtlock));
		c1 = lsm->c1;
		lsm->c1 = NULL;
		assert(!pthread_rwlock_unlock(&lsm->mtlock));

		lsm->policy->destroy(lsm, c1);
	}
	assert(!pthread_rwlock_unlock(&lsm->sblock));
	return err;
}

int myfs_lsm_flush_start(struct myfs_lsm *lsm)
{
	int err;

	myfs_lsm_start_merge(lsm, 0, 0);
	if ((err = __myfs_lsm_flush_start(lsm)))
		myfs_lsm_finish_merge(lsm, 0, 0);
	return err;
}

int myfs_lsm_flush_finish(struct myfs_lsm *lsm)
{
	const int err = __myfs_lsm_flush_finish(lsm);

	myfs_lsm_finish_merge(lsm, 0, 0);
	return err;
}

int myfs_lsm_flush(struct myfs_lsm *lsm)
{
	int err;

	myfs_lsm_start_merge(lsm, 0, 0);
	err = __myfs_lsm_flush_start(lsm);
	if (!err)
		err = __myfs_lsm_flush_finish(lsm);
	myfs_lsm_finish_merge(lsm, 0, 0);
	return err;
}



struct myfs_mtree *myfs_lsm_create_default(struct myfs_lsm *lsm)
{
	struct myfs_skiplist *skip = malloc(sizeof(*skip));

	assert(skip);
	myfs_skiplist_setup(skip, lsm->key_ops->cmp);
	return &skip->mtree;
}

void myfs_lsm_destroy_default(struct myfs_lsm *lsm, struct myfs_mtree *mtree)
{
	(void) lsm;

	struct myfs_skiplist *skip = (struct myfs_skiplist *)mtree;

	myfs_skiplist_release(skip);
	free(skip);
}
