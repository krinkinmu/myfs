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

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>


struct myfs_skip_test {
	int (*test)(void);
	const char *name;
};


static uint64_t skip_hash(uint64_t key)
{
	return ((key + 13) * 188748146801ull) % 2549536629329ull;
}

static uint64_t skip_value(uint64_t key)
{
	return key * 2 + 1;
}

static int skip_key_cmp(const struct myfs_key *l, const struct myfs_key *r)
{
	uint64_t lv, rv;

	memcpy(&lv, l->data, l->size);
	memcpy(&rv, r->data, r->size);

	if (lv != rv)
		return lv < rv ? -1 : 1;
	return 0;
}


struct skip_query {
	struct myfs_query query;
	const struct myfs_key *key;
	const struct myfs_value *value;
};


static int skip_query_cmp(struct myfs_query *q, const struct myfs_key *key)
{
	struct skip_query *query = (struct skip_query *)q;

	return skip_key_cmp(key, query->key);
}

static int skip_query_emit(struct myfs_query *q, const struct myfs_key *key,
			const struct myfs_value *value)
{
	struct skip_query *query = (struct skip_query *)q;

	if (key->size != query->key->size) {
		fprintf(stderr, "wrong key size\n");
		return -EINVAL;
	}

	if (value->size != query->value->size) {
		fprintf(stderr, "wrong value size\n");
		return -EINVAL;
	}

	if (memcmp(key->data, query->key->data, key->size)) {
		fprintf(stderr, "wrong key\n");
		return -EINVAL;
	}

	if (memcmp(value->data, query->value->data, value->size)) {
		fprintf(stderr, "wrong value\n");
		return -EINVAL;
	}

	return 1;
}

static int skip_lookup(struct myfs_skiplist *tree, const struct myfs_key *key,
			const struct myfs_value *value)
{
	struct skip_query query = {
		{ &skip_query_cmp, &skip_query_emit },
		key, value
	};

	return myfs_skip_lookup(tree, &query.query);
}

static int skip_check_content(struct myfs_skiplist *tree, size_t count)
{
	int err = 0;

	for (size_t i = 0; i != count; ++i, err = 0) {
		const uint64_t key = skip_hash(i);
		const uint64_t value = skip_value(key);
		const struct myfs_key k = { sizeof(key), (void *)&key };
		const struct myfs_value v = { sizeof(value), (void *)&value };

		err = skip_lookup(tree, &k, &v);
		if (err < 0)
			break;

		if (!err) {
			err = -ENOENT;
			break;
		}
	}
	return err;
}

static int skip_create(struct myfs_skiplist *tree, size_t count)
{
	int err = 0;

	for (size_t i = 0; i != count; ++i) {
		const uint64_t k = skip_hash(i);
		const uint64_t v = skip_value(k);
		const struct myfs_key key = { sizeof(k), (void *)&k };
		const struct myfs_value value = { sizeof(v), (void *)&v };

		err = myfs_skip_insert(tree, &key, &value);
		if (err)
			break;
	}
	return err;
}

static int skip_insert_test(void)
{
	const size_t ENTRIES = 1000000;

	struct myfs_skiplist tree;
	int err = 0;

	myfs_skiplist_setup(&tree, &skip_key_cmp);
	err = skip_create(&tree, ENTRIES);
	if (!err)
		err = skip_check_content(&tree, ENTRIES);
	myfs_skiplist_release(&tree);

	return err;
}

static int skip_update_test(void)
{
	const size_t ROUND = 1000;
	const size_t ROUNDS = 1000;

	struct myfs_skiplist tree;
	int err = 0;

	myfs_skiplist_setup(&tree, &skip_key_cmp);
	for (size_t i = 0; !err && i != ROUNDS; ++i) {

		for (size_t j = 0; j != ROUND; ++j) {
			const uint64_t k = skip_hash(j);
			const uint64_t v = i;
			const struct myfs_key key = {
				sizeof(k), (void *)&k
			};
			const struct myfs_value value = {
				sizeof(v), (void *)&v
			};

			err = myfs_skip_insert(&tree, &key, &value);
			if (err)
				break;
		}

		for (size_t j = 0; !err && j != ROUND; ++j, err = 0) {
			const uint64_t key = skip_hash(j);
			const uint64_t value = i;

			const struct myfs_key k = {
				sizeof(key), (void *)&key
			};
			const struct myfs_value v = {
				sizeof(value), (void *)&value
			};

			err = skip_lookup(&tree, &k, &v);
			if (err < 0)
				break;

			if (!err) {
				err = -ENOENT;
				break;
			}
		}
	}
	myfs_skiplist_release(&tree);

	return err;
}

static int run_tests(void)
{
	const struct myfs_skip_test test[] = {
		{ &skip_insert_test, "skip_insert_test" },
		{ &skip_update_test, "skip_update_test" },
	};

	for (int i = 0; i != sizeof(test)/sizeof(test[0]); ++i) {
		const int err = test[i].test();

		if (!err)
			continue;

		fprintf(stderr, "test %s failed (%d)\n", test[i].name, err);
		return err;
	}
	return 0;
}

int main(void)
{
	const int ret = run_tests();

	return ret ? 1 : 0;
}
