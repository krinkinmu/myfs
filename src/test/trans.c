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
#include <trans/trans.h>
#include <myfs.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#include <stdio.h>
#include <errno.h>


static const char *TEST_NAME = "test.bin";
static const size_t ENTRIES = 100000000;


struct myfs_trans_test {
	int (*test)(struct myfs *, struct myfs_trans *);
	const char *name;
};


static unsigned get_type(size_t i)
{
	return (unsigned)i;
}

static uint64_t get_key(size_t i)
{
	return i * 3 + 1;
}

static uint64_t get_value(size_t i)
{
	return i * 3 + 2;
}

static int myfs_write_test(struct myfs *myfs, struct myfs_trans *tr)
{
	struct myfs_log log;
	int err = 0;

	myfs_log_setup(&log);
	for (size_t i = 0; i != ENTRIES; ++i) {
		const unsigned type = get_type(i);
		const uint64_t key = get_key(i);
		const uint64_t value = get_value(i);

		const struct myfs_key k = {
			.size = sizeof(key),
			.data = (void *)&key
		};
		const struct myfs_value v = {
			.size = sizeof(value),
			.data = (void *)&value
		};

		err = myfs_log_append(myfs, &log, type, &k, &v);
		if (err)
			break;
	}

	if (!err && !(err = myfs_log_finish(myfs, &log)))
		myfs_log_register(tr, &log);

	myfs_log_release(&log);
	return err;
}


struct myfs_trans_check {
	struct myfs_trans_scanner scanner;
	size_t i;
};


static int myfs_trans_emit(struct myfs_trans_scanner *scanner,
			unsigned type,
			const struct myfs_key *key,
			const struct myfs_value *value)
{
	struct myfs_trans_check *check = (struct myfs_trans_check *)scanner;

	if (check->i == ENTRIES)
		return -EFBIG;

	const unsigned t = get_type(check->i);
	const uint64_t k = get_key(check->i);
	const uint64_t v = get_value(check->i);

	if (key->size != sizeof(k)) {
		fprintf(stderr, "Wrong key size\n");
		return -EINVAL;
	}

	if (value->size != sizeof(v)) {
		fprintf(stderr, "Wrong value size\n");
		return -EINVAL;
	}

	if (t != type) {
		fprintf(stderr, "Wrong type\n");
		return -EINVAL;
	}

	if (k != *(uint64_t *)key->data) {
		fprintf(stderr, "Wrong key\n");
		return -EINVAL;
	}

	if (v != *(uint64_t *)value->data) {
		fprintf(stderr, "Wrong value\n");
		return -EINVAL;
	}

	++check->i;
	return 0;
}


static int myfs_read_test(struct myfs *myfs, struct myfs_trans *tr)
{
	struct myfs_trans_check check = {
		{ &myfs_trans_emit },
		0
	};

	const int ret = myfs_trans_scan(myfs, tr, &check.scanner);

	if (ret)
		return ret;


	if (check.i != ENTRIES) {
		fprintf(stderr, "unexpected WAL end\n");
		return -ENOENT;
	}
	return 0;
}


static int run_tests(struct myfs *myfs)
{
	const struct myfs_trans_test test[] = {
		{ &myfs_write_test, "myfs_write_test" },
		{ &myfs_read_test, "myfs_read_test" },
	};

	struct myfs_trans tr;

	myfs_trans_setup(&tr);
	for (int i = 0; i != sizeof(test)/sizeof(test[0]); ++i) {
		const int err = test[i].test(myfs, &tr);

		if (!err)
			continue;

		fprintf(stderr, "test %s failed (%d)\n", test[i].name, err);
		return err;
	}
	myfs_trans_release(&tr);
	return 0;
}

int main(void)
{
	const int fd = open(TEST_NAME, O_RDWR | O_CREAT | O_TRUNC,
				S_IRUSR | S_IWUSR);

	if (fd < 0) {
		perror("failed to create test.bin file");
		return 1;
	}

	struct sync_bdev bdev;
	struct myfs myfs;

	sync_bdev_setup(&bdev, fd);
	myfs.bdev = &bdev.bdev;
	myfs.page_size = 4096;
	myfs.next_offs = 0;

	const int ret = run_tests(&myfs);

	if (ret)
		fprintf(stderr, "tests failed\n");
	else
		unlink(TEST_NAME);
	close(fd);

	return ret ? 1 : 0;
}
