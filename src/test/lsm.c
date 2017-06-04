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
#include <block/block.h>
#include <lsm/lsm.h>
#include <myfs.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <getopt.h>
#include <unistd.h>
#include <fcntl.h>

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>


static const size_t COUNT = 1000000;


struct myfs_lsm_test {
	int (*test)(struct myfs *, struct myfs_lsm_sb *);
	const char *name;
};

struct myfs_lsm_key {
	uint64_t key;
	int deleted;
};

struct lsm_query {
	struct myfs_query query;
	const struct myfs_key *key;
	const struct myfs_value *value;
};

struct lsm_range_query {
	struct myfs_query query;
	uint64_t from;
	uint64_t to;
	uint64_t next;
};


static int lsm_key_cmp(const struct myfs_key *l, const struct myfs_key *r)
{
	struct myfs_lsm_key lv, rv;

	assert(l->size == sizeof(lv));
	assert(r->size == sizeof(rv));
	memcpy(&lv, l->data, l->size);
	memcpy(&rv, r->data, r->size);

	if (lv.key != rv.key)
		return lv.key < rv.key ? -1 : 1;
	return 0;
}

static int lsm_key_deleted(const struct myfs_key *key,
			const struct myfs_value *value)
{
	struct myfs_lsm_key k;

	(void) value;
	assert(key->size == sizeof(k));
	memcpy(&k, key->data, key->size);
	return k.deleted;
}

static const struct myfs_key_ops lsm_key_ops = {
	.cmp = &lsm_key_cmp,
	.deleted = &lsm_key_deleted
};


static int lsm_query_cmp(struct myfs_query *q, const struct myfs_key *key)
{
	struct lsm_query *query = (struct lsm_query *)q;

	return lsm_key_cmp(key, query->key);
}

static int lsm_range_cmp(struct myfs_query *q, const struct myfs_key *key)
{
	struct lsm_range_query *range = (struct lsm_range_query *)q;
	struct myfs_lsm_key value;

	assert(key->size == sizeof(value));
	memcpy(&value, key->data, key->size);

	if (value.key < range->from)
		return -1;
	if (value.key >= range->to)
		return 1;
	return 0;
}

static int lsm_query_emit(struct myfs_query *q, const struct myfs_key *key,
			const struct myfs_value *value)
{
	struct lsm_query *query = (struct lsm_query *)q;

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
		abort();
		return -EINVAL;
	}

	if (memcmp(value->data, query->value->data, value->size)) {
		fprintf(stderr, "wrong value\n");
		return -EINVAL;
	}

	return 1;
}

static int lsm_range_emit(struct myfs_query *q, const struct myfs_key *key,
			const struct myfs_value *value)
{
	struct lsm_range_query *query = (struct lsm_range_query *)q;
	struct myfs_lsm_key k;

	if (key->size != sizeof(k)) {
		fprintf(stderr, "wrong key size\n");
		return -EINVAL;
	}

	if (value->size != sizeof(k)) {
		fprintf(stderr, "wrong value size\n");
		return -EINVAL;
	}

	memcpy(&k, key->data, key->size);
	if (k.key != query->next) {
		fprintf(stderr, "wrong key\n");
		return -EINVAL;	
	}

	memcpy(&k, value->data, value->size);
	if (k.key != query->next) {
		fprintf(stderr, "wrong value\n");
		return -EINVAL;	
	}
	++query->next;
	return 0;
}

static void lsm_setup(struct myfs *myfs, struct myfs_lsm *lsm,
			const struct myfs_lsm_sb *sb)
{
	myfs_lsm_setup(lsm, myfs, &myfs_lsm_default_policy, &lsm_key_ops, sb);
}

static void lsm_release(struct myfs_lsm *lsm)
{
	myfs_lsm_release(lsm);
}

static int lsm_lookup(struct myfs_lsm *lsm, const struct myfs_key *key,
			const struct myfs_value *val)
{
	struct lsm_query query = {
		{ &lsm_query_cmp, &lsm_query_emit },
		key, val
	};

	return myfs_lsm_lookup(lsm, &query.query);
}

static int lsm_range(struct myfs_lsm *lsm, uint64_t from, uint64_t to)
{
	struct lsm_range_query query = {
		{ &lsm_range_cmp, &lsm_range_emit },
		from, to, from
	};
	const int err = myfs_lsm_range(lsm, &query.query);

	if (err)
		return err;
	if (query.next != query.to) {
		fprintf(stderr, "Unexpected end of the sequence\n");
		return -ENOENT;
	}
	return 0;
}


static int lsm_insert_rnd_test(struct myfs *myfs, struct myfs_lsm_sb *sb)
{
	struct myfs_lsm lsm;
	int err = 0;

	struct myfs_lsm_key *ks = calloc(COUNT, sizeof(*ks));

	assert(ks);
	for (size_t i = 0; i != COUNT; ++i)
		ks[i].key = i;

	for (size_t i = 0; i != COUNT - 1; ++i) {
		const size_t pos = i + (rand() % (COUNT - i));
		const struct myfs_lsm_key tmp = ks[pos];

		ks[pos] = ks[i];
		ks[i] = tmp;
	}


	memset(sb, 0, sizeof(*sb));
	lsm_setup(myfs, &lsm, sb);
	for (size_t i = 0; !err && i != COUNT; ++i) {
		const struct myfs_key key = { sizeof(ks[i]), (void *)&ks[i] };
		const struct myfs_value val = { sizeof(ks[i]), (void *)&ks[i] };

		err = myfs_lsm_insert(&lsm, &key, &val);

		if (!err && myfs_lsm_need_flush(&lsm))
			err = myfs_lsm_flush(&lsm);	
		if (!err && myfs_lsm_need_merge(&lsm, 0))
			err = myfs_lsm_merge(&lsm, 0);
		if (!err && myfs_lsm_need_merge(&lsm, 1))
			err = myfs_lsm_merge(&lsm, 1);
		if (!err && myfs_lsm_need_merge(&lsm, 2))
			err = myfs_lsm_merge(&lsm, 2);
	}
	if (!err)
		err = myfs_lsm_flush(&lsm);
	memcpy(sb, &lsm.sb, sizeof(*sb));
	lsm_release(&lsm);
	free(ks);
	return err;
}

static int lsm_lookup_seq_test(struct myfs *myfs, struct myfs_lsm_sb *sb)
{
	struct myfs_lsm_key k;
	const struct myfs_key key = { sizeof(k), (void *)&k };
	const struct myfs_value val = { sizeof(k), (void *)&k };

	struct myfs_lsm lsm;
	int err = 0;

	memset(&k, 0, sizeof(k));
	lsm_setup(myfs, &lsm, sb);
	for (size_t i = 0; i != COUNT; ++i) {
		k.key = i;
		k.deleted = 0;

		err = lsm_lookup(&lsm, &key, &val);
		if (err < 0)
			break;

		if (!err) {
			fprintf(stderr, "failed to find entry\n");
			err = -ENOENT;
			break;
		}
		err = 0;
	}
	lsm_release(&lsm);
	return err;
}

static int lsm_lookup_rnd_test(struct myfs *myfs, struct myfs_lsm_sb *sb)
{
	struct myfs_lsm_key k;
	const struct myfs_key key = { sizeof(k), (void *)&k };
	const struct myfs_value value = { sizeof(k), (void *)&k };

	struct myfs_lsm lsm;
	int err = 0;

	memset(&k, 0, sizeof(k));
	lsm_setup(myfs, &lsm, sb);
	for (size_t i = 0; i != COUNT; ++i) {
		k.key = rand() % COUNT;
		k.deleted = 0;
		err = lsm_lookup(&lsm, &key, &value);
		if (err < 0)
			break;
		if (!err) {
			fprintf(stderr, "failed to find entry\n");
			err = -ENOENT;
			break;
		}
		err = 0;
	}
	lsm_release(&lsm);
	return err;
}

static int lsm_lookup_range_test(struct myfs *myfs, struct myfs_lsm_sb *sb)
{
	struct myfs_lsm lsm;
	int err;

	lsm_setup(myfs, &lsm, sb);
	err = lsm_range(&lsm, 0, COUNT);
	lsm_release(&lsm);
	return err;
}

static int lsm_remove_seq_test(struct myfs *myfs, struct myfs_lsm_sb *sb)
{
	struct myfs_lsm_key k;
	const struct myfs_key key = { sizeof(k), (void *)&k };
	const struct myfs_value value = { sizeof(k), (void *)&k };
	struct myfs_lsm lsm;
	int err = 0;

	memset(&k, 0, sizeof(k));
	lsm_setup(myfs, &lsm, sb);
	for (size_t i = 0; !err && i != COUNT; ++i) {
		k.key = i;
		k.deleted = 1;
		err = myfs_lsm_insert(&lsm, &key, &value);

		if (!err && myfs_lsm_need_flush(&lsm))
			err = myfs_lsm_flush(&lsm);	
		if (!err && myfs_lsm_need_merge(&lsm, 0))
			err = myfs_lsm_merge(&lsm, 0);
		if (!err && myfs_lsm_need_merge(&lsm, 1))
			err = myfs_lsm_merge(&lsm, 1);
		if (!err && myfs_lsm_need_merge(&lsm, 2))
			err = myfs_lsm_merge(&lsm, 2);
	}
	if (!err)
		err = myfs_lsm_flush(&lsm);
	memcpy(sb, &lsm.sb, sizeof(*sb));
	lsm_release(&lsm);
	return err;
}

static int run_tests(struct myfs *myfs)
{
	const struct myfs_lsm_test test[] = {
		{ &lsm_insert_rnd_test, "lsm_insert random" },
		{ &lsm_lookup_seq_test, "lsm_lookup sequential" },
		{ &lsm_lookup_rnd_test, "lsm_lookup random" },
		{ &lsm_lookup_range_test, "lsm_lookup_range" },
		{ &lsm_remove_seq_test, "lsm_remove sequential" },
	};
	struct myfs_lsm_sb sb;

	for (int i = 0; i != sizeof(test)/sizeof(test[0]); ++i) {
		const int err = test[i].test(myfs, &sb);

		if (!err)
			continue;

		fprintf(stderr, "test %s failed (%d)\n", test[i].name, err);
		return err;
	}
	return 0;
}


static const char *TEST_NAME = "test.bin";

static const struct option opts[] = {
	{"fanout", required_argument, NULL, 'f'},
	{NULL, 0, NULL, 0},
};

int main(int argc, char **argv)
{
	size_t fanout = MYFS_MIN_FANOUT;
	char *endptr;
	int kind;

	while ((kind = getopt_long(argc, argv, "f:", opts, NULL)) != -1) {
		switch (kind) {
		case 'f':
			fanout = strtoul(optarg, &endptr, 10);
			if (*endptr != '\0') {
				fprintf(stderr, "fanout must be a number\n");
				return -1;
			}
			break;
		default:
			fprintf(stderr, "unexpected argument\n");
			return -1;
		}
	}

	const int fd = open(TEST_NAME, O_RDWR | O_CREAT | O_TRUNC,
				S_IRUSR | S_IWUSR);

	if (fd < 0) {
		perror("failed to create test file");
		return 1;
	}

	struct sync_bdev bdev;
	struct myfs myfs;

	sync_bdev_setup(&bdev, fd);
	myfs.bdev = &bdev.bdev;
	myfs.page_size = 4096;
	myfs.fanout = fanout;
	myfs.next_offs = 0;

	const int ret = run_tests(&myfs);

	if (ret)
		fprintf(stderr, "tests failed\n");
	else
		unlink(TEST_NAME);
	close(fd);

	return ret ? 1 : 0;
}
