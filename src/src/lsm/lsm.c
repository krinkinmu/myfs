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



struct myfs_range_query {
	struct myfs_query proxy;
	struct myfs_query *orig;
	struct myfs_items *items;
};


static int myfs_range_cmp(struct myfs_query *p, const struct myfs_key *key)
{
	struct myfs_range_query *proxy = (struct myfs_range_query *)p;

	if (!proxy->orig)
		return 0;
	return proxy->orig->cmp(proxy->orig, key);
}

static int myfs_range_emit(struct myfs_query *p, const struct myfs_key *key,
			const struct myfs_value *value)
{
	struct myfs_range_query *proxy = (struct myfs_range_query *)p;

	myfs_items_append(proxy->items, key, value);
	return 0;
}

static void myfs_range_setup(struct myfs_range_query *query,
			struct myfs_query *orig,
			struct myfs_items *items)
{
	memset(query, 0, sizeof(*query));
	query->proxy.cmp = &myfs_range_cmp;
	query->proxy.emit = &myfs_range_emit;
	query->orig = orig;
	query->items = items;
}



struct myfs_merge_ctx {
	struct myfs_lsm *lsm;

	struct myfs_items m[2];
	size_t mpos[2];

	struct myfs_ctree_it it[MYFS_MAX_TREES];
	struct myfs_query *query;

	int index;
	struct myfs_key key;
	struct myfs_value value;
};


static void __myfs_merge_setup(struct myfs_merge_ctx *ctx, struct myfs_lsm *lsm)
{
	static const struct myfs_ctree_sb empty;

	memset(ctx, 0, sizeof(*ctx));
	ctx->lsm = lsm;
	myfs_items_setup(&ctx->m[0]);
	myfs_items_setup(&ctx->m[1]);
	for (int i = 0; i != MYFS_MAX_TREES; ++i)
		myfs_ctree_it_setup(&ctx->it[i], &empty);
}

static int myfs_prepare_range(struct myfs_merge_ctx *ctx, struct myfs_lsm *lsm,
			struct myfs_query *query)
{
	struct myfs_range_query proxy[2];
	struct myfs *myfs = lsm->myfs;
	int err;

	__myfs_merge_setup(ctx, lsm);
	ctx->query = query;

	myfs_range_setup(&proxy[0], query, &ctx->m[0]);
	myfs_range_setup(&proxy[1], query, &ctx->m[1]);

	assert(!pthread_rwlock_rdlock(&lsm->mtlock));
	err = lsm->c0->range(lsm->c0, &proxy[0].proxy);
	if (!err && lsm->c1)
		err = lsm->c1->range(lsm->c1, &proxy[1].proxy);
	assert(!pthread_rwlock_unlock(&lsm->mtlock));

	if (err)
		return err;

	assert(!pthread_rwlock_rdlock(&lsm->sblock));
	for (int i = 0; i != MYFS_MAX_TREES; ++i)
		myfs_ctree_it_setup(&ctx->it[i], &lsm->sb.tree[i]);
	assert(!pthread_rwlock_unlock(&lsm->sblock));

	for (int i = 0; i != MYFS_MAX_TREES; ++i) {
		err = myfs_ctree_it_find(myfs, &ctx->it[i], query);
		if (err)
			return err;
	}
	return 0;
}

static int myfs_prepare_flush(struct myfs_merge_ctx *ctx, struct myfs_lsm *lsm,
			struct myfs_mtree *new, const struct myfs_ctree_sb *old)
{
	struct myfs_range_query proxy;
	int err;

	__myfs_merge_setup(ctx, lsm);

	myfs_range_setup(&proxy, NULL, &ctx->m[1]);
	err = new->scan(new, &proxy.proxy);
	if (err)
		return err;

	myfs_ctree_it_setup(&ctx->it[0], old);
	return myfs_ctree_it_reset(lsm->myfs, &ctx->it[0]);
}

static int myfs_prepare_merge(struct myfs_merge_ctx *ctx, struct myfs_lsm *lsm,
			const struct myfs_ctree_sb *new,
			const struct myfs_ctree_sb *old)
{
	struct myfs *myfs = lsm->myfs;
	int err;

	__myfs_merge_setup(ctx, lsm);

	myfs_ctree_it_setup(&ctx->it[0], new);
	myfs_ctree_it_setup(&ctx->it[1], old);

	err = myfs_ctree_it_reset(myfs, &ctx->it[0]);
	if (!err)
		err = myfs_ctree_it_reset(myfs, &ctx->it[1]);
	return err;
}

static void myfs_merge_release(struct myfs_merge_ctx *ctx)
{
	myfs_items_release(&ctx->m[1]);
	myfs_items_release(&ctx->m[0]);
	for (int i = 0; i != MYFS_MAX_TREES; ++i)
		myfs_ctree_it_release(&ctx->it[i]);
	memset(ctx, 0, sizeof(*ctx));
}

static int myfs_merge_advance(struct myfs_merge_ctx *ctx)
{
	struct myfs_lsm *lsm = ctx->lsm;
	struct myfs *myfs = lsm->myfs;
	const myfs_cmp_t cmp = lsm->key_ops->cmp;


	for (int i = 0; i != 2; ++i) {
		if (ctx->mpos[i] == ctx->m[i].size)
			continue;

		struct myfs_value v;
		struct myfs_key k;

		myfs_items_get(&ctx->m[i], ctx->mpos[i], &k, &v);
		if (!cmp(&ctx->key, &k))
			++ctx->mpos[i];
	}

	for (int i = 0; i != MYFS_MAX_TREES; ++i) {
		struct myfs_ctree_it *it = &ctx->it[i];

		/**
		 * Moving ctree iterator invalidates key/value pointers,
		 * so in the case when current value came from a disk
		 * tree we need to skip the tree until we don't need the
		 * key/value pair.
		 **/
		if (i + 2 == ctx->index)
			continue;

		if (!myfs_ctree_it_valid(it))
			continue;

		if (ctx->query && ctx->query->cmp(ctx->query, &it->key))
			continue;

		if (!cmp(&ctx->key, &it->key)) {
			const int err = myfs_ctree_it_next(myfs, it);

			if (err)
				return err;
		}
	}

	if (ctx->index >= 2) {
		struct myfs_ctree_it *it = &ctx->it[ctx->index - 2];
		const int err = myfs_ctree_it_next(myfs, it);

		if (err)
			return err;
	}

	return 0;
}

static int myfs_merge_next(struct myfs_merge_ctx *ctx)
{
	struct myfs_lsm *lsm = ctx->lsm;
	const myfs_cmp_t cmp = lsm->key_ops->cmp;

	struct myfs_key key = { 0, NULL };
	struct myfs_value value = { 0, NULL };
	int index = 0;


	if (ctx->key.data) {
		const int err = myfs_merge_advance(ctx);

		if (err)
			return err;
	}

	for (int i = 0; i != 2; ++i) {
		if (ctx->mpos[i] == ctx->m[i].size)
			continue;

		struct myfs_value v;
		struct myfs_key k;

		myfs_items_get(&ctx->m[i], ctx->mpos[i], &k, &v);
		if (!key.data || cmp(&k, &key) < 0) {
			key = k;
			value = v;
			index = i;
		}
	}

	for (int i = 0; i != MYFS_MAX_TREES; ++i) {
		struct myfs_ctree_it *it = &ctx->it[i];

		if (!myfs_ctree_it_valid(it))
			continue;

		if (!key.data || cmp(&it->key, &key) < 0) {
			key = it->key;
			value = it->value;
			index = i + 2;
		}
	}

	ctx->key = key;
	ctx->value = value;
	ctx->index = index;

	return key.data ? 1 : 0;
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
		assert(!pthread_rwlock_unlock(&lsm->sblock));

		err = myfs_ctree_lookup(myfs, &sb, &proxy.proxy);
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



int myfs_lsm_range_default(struct myfs_lsm *lsm, struct myfs_query *query)
{
	struct myfs_merge_ctx ctx;
	int err = myfs_prepare_range(&ctx, lsm, query);

	while ((err = myfs_merge_next(&ctx)) == 1) {
		if (lsm->key_ops->deleted(&ctx.key, &ctx.value))
			continue;

		err = query->emit(query, &ctx.key, &ctx.value);
		if (err)
			break;
	}
	myfs_merge_release(&ctx);
	return err;
}


int myfs_lsm_flush_default(struct myfs_lsm *lsm, int drop_deleted,
			struct myfs_mtree *new,
			const struct myfs_ctree_sb *old,
			struct myfs_ctree_sb *res)
{
	struct myfs *myfs = lsm->myfs;
	struct myfs_merge_ctx ctx;
	int err;

	err = myfs_prepare_flush(&ctx, lsm, new, old);
	if (!err) {
		struct myfs_ctree_builder build;

		myfs_builder_setup(&build);
		while ((err = myfs_merge_next(&ctx)) == 1) {
			struct myfs_key *key = &ctx.key;
			struct myfs_value *value = &ctx.value;

			if (drop_deleted && lsm->key_ops->deleted(key, value))
				continue;
			err = myfs_builder_append(myfs, &build, key, value);
			if (err)
				break;
		}
		if (!err)
			err = myfs_builder_finish(myfs, &build);
		if (!err)
			*res = build.sb;
		myfs_builder_release(&build);
	}
	myfs_merge_release(&ctx);
	return err;
}

int myfs_lsm_merge_default(struct myfs_lsm *lsm, int drop_deleted,
			const struct myfs_ctree_sb *new,
			const struct myfs_ctree_sb *old,
			struct myfs_ctree_sb *res)
{
	struct myfs *myfs = lsm->myfs;
	struct myfs_merge_ctx ctx;
	int err;

	err = myfs_prepare_merge(&ctx, lsm, new, old);
	if (!err) {
		struct myfs_ctree_builder build;

		myfs_builder_setup(&build);
		while ((err = myfs_merge_next(&ctx)) == 1) {
			struct myfs_key *key = &ctx.key;
			struct myfs_value *value = &ctx.value;

			if (drop_deleted && lsm->key_ops->deleted(key, value))
				continue;
			err = myfs_builder_append(myfs, &build, key, value);
			if (err)
				break;
		}
		if (!err)
			err = myfs_builder_finish(myfs, &build);
		if (!err)
			*res = build.sb;
		myfs_builder_release(&build);
	}
	myfs_merge_release(&ctx);
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
		const int err = lsm->policy->merge(lsm, lsm->size <= i + 2,
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
		err = lsm->policy->flush(lsm, lsm->size <= 1,
					lsm->c1, &old, &res);
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
