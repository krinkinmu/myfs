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
#include <wal/wal.h>
#include <myfs.h>

#include <assert.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>

#include <unistd.h>
#include <getopt.h>
#include <fcntl.h>
#include <pthread.h>


static volatile int force_stop;
static struct myfs_wal wal;
static size_t iters = 1000000;
static size_t size = 256;
static size_t threads = 1;


static void *worker(void *unused)
{
	(void) unused;
	void *data = malloc(size);

	assert(data);
	for (size_t i = 0; !force_stop && i != iters; ++i) {
		struct myfs_trans trans;

		myfs_wal_trans_setup(&trans);
		myfs_wal_trans_append(&trans, data, size);
		myfs_wal_trans_finish(&trans);
		assert(!myfs_wal_append(&wal, &trans));
		myfs_wal_trans_release(&trans);
	}
	free(data);
	return NULL;
}


static const char *TEST_NAME = "test.bin";

static const struct option opts[] = {
	{"threads", required_argument, NULL, 't'},
	{"iters", required_argument, NULL, 'i'},
	{"size", required_argument, NULL, 's'},
	{NULL, 0, NULL, 0},
};

int main(int argc, char **argv)
{
	char *endptr;
	int kind;
	size_t i;
	int err = 0;

	while ((kind = getopt_long(argc, argv, "t:i:s:", opts, NULL)) != -1) {
		switch (kind) {
		case 't':
			threads = strtoul(optarg, &endptr, 10);
			if (*endptr != '\0') {
				fprintf(stderr, "threads must be a number\n");
				return -1;
			}
			if (threads < 1) {
				fprintf(stderr, "iters must be > 0\n");
				return -1;
			}
			break;
		case 'i':
			iters = strtoul(optarg, &endptr, 10);
			if (*endptr != '\0') {
				fprintf(stderr, "iters must be a number\n");
				return -1;
			}
			if (iters < 1) {
				fprintf(stderr, "iters must be > 0\n");
				return -1;
			}
			break;
		case 's':
			size = strtoul(optarg, &endptr, 10);
			if (*endptr != '\0') {
				fprintf(stderr, "size must be a number\n");
				return -1;
			}
			if (size < 1) {
				fprintf(stderr, "iters must be > 0\n");
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
		fprintf(stderr, "failed to create test file\n");
		return -1;
	}

	struct sync_bdev bdev;
	struct myfs myfs;

	sync_bdev_setup(&bdev, fd);
	myfs.bdev = &bdev.bdev;
	myfs.page_size = 4096;
	myfs.next_offs = 0;

	pthread_t *th = calloc(threads, sizeof(pthread_t));

	assert(th);
	myfs_wal_setup(&wal, &myfs);
	for (i = 0; i != threads; ++i) {
		if (pthread_create(&th[i], NULL, &worker, NULL)) {
			fprintf(stderr, "failed to create thread %lu\n",
						(size_t)i);
			force_stop = 1;
			err = -1;
			break;
		}
	}
	for (size_t j = 0; j != i; ++j)
		pthread_join(th[j], NULL);
	myfs_wal_release(&wal);
	free(th);

	if (err)
		fprintf(stderr, "tests failed\n");
	else
		unlink(TEST_NAME);
	close(fd);

	return err;
}
