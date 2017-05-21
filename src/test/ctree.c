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
#include <lsm/ctree.h>
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


static const size_t ENTRIES = 100000000;


struct myfs_ctree_test {
	int (*test)(struct myfs *, struct myfs_ctree_sb *sb);
	const char *name;
};

static int ctree_write_test(struct myfs *myfs, struct myfs_ctree_sb *sb)
{
	struct myfs_ctree_builder b;
	int err = 0;

	myfs_builder_setup(&b);
	for (size_t i = 0; i != ENTRIES; ++i) {
		const uint64_t value = 2 * i + 1;
		const uint64_t key = 2 * i;

		const struct myfs_value v = { sizeof(value), (void *)&value };
		const struct myfs_key k = { sizeof(key), (void *)&key };

		err = myfs_builder_append(myfs, &b, &k, &v);
		if (err)
			break;
	}

	err = myfs_builder_finish(myfs, &b);
	*sb = b.sb;
	myfs_builder_release(&b);
	return err;
}

static int ctree_read_test(struct myfs *myfs, struct myfs_ctree_sb *sb)
{
	struct myfs_ctree_it it;
	int err = 0;

	myfs_ctree_it_setup(&it, sb);
	err = myfs_ctree_it_reset(myfs, &it);
	for (size_t i = 0; !err && i != ENTRIES; ++i, err = 0) {
		const uint64_t value = 2 * i + 1;
		const uint64_t key = 2 * i;

		if (!myfs_ctree_it_valid(&it)) {
			fprintf(stderr, "unexpectged CTREE end\n");
			err = -ENOENT;
			break;
		}

		if (it.key.size != sizeof(key)) {
			fprintf(stderr, "wrong key size\n");
			err = -EINVAL;
			break;
		}

		if (it.value.size != sizeof(value)) {
			fprintf(stderr, "wrong value size\n");
			err = -EINVAL;
			break;
		}

		if (memcmp(&key, it.key.data, sizeof(key))) {
			fprintf(stderr, "wrong key\n");
			err = -EINVAL;
			break;
		}

		if (memcmp(&value, it.value.data, sizeof(value))) {
			fprintf(stderr, "wrong value\n");
			err = -EINVAL;
			break;
		}

		err = myfs_ctree_it_next(myfs, &it);
		if (err < 0) {
			fprintf(stderr, "myfs_ctree_it_next failed\n");
			break;
		}
	}
	myfs_ctree_it_release(&it);
	return err;
}


struct ctree_key_query {
	struct myfs_query query;
	const struct myfs_key *key;
	const struct myfs_value *value;
};


static int ctree_key_cmp(const struct myfs_key *l, const struct myfs_key *r)
{
	assert(l->size == r->size && l->size == sizeof(uint64_t));
	const uint64_t lk = *((const uint64_t *)l->data);
	const uint64_t rk = *((const uint64_t *)r->data);

	if (lk != rk)
		return lk < rk ? -1 : 1;
	return 0;
}

static int ctree_query_cmp(struct myfs_query *q, const struct myfs_key *key)
{
	struct ctree_key_query *query = (struct ctree_key_query *)q;

	return ctree_key_cmp(key, query->key);
}

static int ctree_query_emit(struct myfs_query *q, const struct myfs_key *key,
			const struct myfs_value *value)
{
	struct ctree_key_query *query = (struct ctree_key_query *)q;

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

static int ctree_lookup(struct myfs *myfs, const struct myfs_ctree_sb *sb,
			const struct myfs_key *key,
			const struct myfs_value *value)
{
	struct ctree_key_query query = {
		{ &ctree_query_cmp, &ctree_query_emit },
		key, value
	};

	return myfs_ctree_lookup(myfs, sb, &query.query);
}


static int ctree_lookup_test(struct myfs *myfs, struct myfs_ctree_sb *sb)
{
	int err = 0;

	for (size_t i = 0; i != ENTRIES; ++i, err = 0) {
		const uint64_t key = rand() % (2 * ENTRIES);
		const uint64_t value = key + 1;
		const struct myfs_key k = { sizeof(key), (void *)&key };
		const struct myfs_value v = { sizeof(value), (void *)&value };

		err = ctree_lookup(myfs, sb, &k, &v); 

		if (err < 0)
			break;

		if (!err && (key % 2 == 0)) {
			fprintf(stderr, "key not found\n");
			err = -ENOENT;
			break;
		}

		if (err && (key % 2 == 1)) {
			fprintf(stderr, "unexpected key found\n");
			err = -EINVAL;
			break;
		}
	}
	return err;
}

static int run_tests(struct myfs *myfs)
{
	const struct myfs_ctree_test test[] = {
		{ &ctree_write_test, "ctree_write_test" },
		{ &ctree_read_test, "ctree_read_test" },
		{ &ctree_lookup_test, "ctree_lookup_test" },
	};
	struct myfs_ctree_sb sb;

	memset(&sb, 0, sizeof(sb));
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
