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
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>


static int sync_write(int fd, const char *buf, size_t size, off_t offs)
{
	while (size) {
		const ssize_t ret = pwrite(fd, buf, size, offs);

		if (ret < 0)
			return errno;

		offs += ret;
		size -= ret;
	}
	return 0;
}

static int sync_read(int fd, char *buf, size_t size, off_t offs)
{
	while (size) {
		const ssize_t ret = pread(fd, buf, size, offs);

		if (ret < 0)
			return -errno;
		if (!ret)
			return -EINVAL;

		offs += ret;
		size -= ret;
	}
	return 0;
}

static void sync_bdev_handle(struct bio *bio)
{
	struct sync_bdev *bdev = (struct sync_bdev *)bio->bdev;
	size_t i;

	if ((bio->flags & BIO_RWDIR) == BIO_WRITE) {
		for (i = 0; i != bio->cnt; ++i) {
			const struct bio_vec *vec = &bio->vec[i];
			const int err = sync_write(bdev->fd, vec->buf,
						vec->size, vec->offs);

			if (err) {
				bio->err = err;
				break;
			}
		}
	} else {
		for (i = 0; i != bio->cnt; ++i) {
			struct bio_vec *vec = &bio->vec[i];
			const int err = sync_read(bdev->fd, vec->buf,
						vec->size, vec->offs);

			if (err) {
				bio->err = err;
				break;
			}
		}
	}

	if (!bio->err && bio->flags & BIO_SYNC)
		syncfs(bdev->fd);
	bio_complete(bio);
}

static size_t sync_bdev_size(struct bdev *bdev)
{
	struct sync_bdev *sbdev = (struct sync_bdev *)bdev;
	struct stat buf;

	assert(!fstat(sbdev->fd, &buf));
	return buf.st_size;
}

void sync_bdev_setup(struct sync_bdev *bdev, int fd)
{
	bdev->bdev.handle = &sync_bdev_handle;
	bdev->bdev.size = &sync_bdev_size;
	bdev->fd = fd;
}

size_t bdev_size(struct bdev *bdev)
{
	return bdev->size(bdev);
}


void bio_setup(struct bio *bio, struct bdev *bdev)
{
	memset(bio, 0, sizeof(*bio));
	bio->bdev = bdev;
	bio->vec = bio->_inline;
	bio->cap = sizeof(bio->_inline) / sizeof(bio->_inline[0]);

	assert(!pthread_mutex_init(&bio->mtx, NULL));
	assert(!pthread_cond_init(&bio->cv, NULL));
}

void bio_release(struct bio *bio)
{
	if (bio->vec != bio->_inline)
		free(bio->vec);
	assert(!pthread_mutex_destroy(&bio->mtx));
	assert(!pthread_cond_destroy(&bio->cv));
}

void bio_add_vec(struct bio *bio, void *buf, uint64_t offs, uint64_t size)
{
	const uint64_t mask = (1ull << 9) - 1;
	const size_t init = 16;
	const int resize = 2;

	assert(!(offs & mask) && !(size & mask));

	if (bio->cnt == bio->cap) {
		const size_t cap = bio->cap ? bio->cap * resize : init;
		const size_t size = cap * sizeof(*bio->vec);

		if (bio->vec == bio->_inline)
			assert(bio->vec = malloc(size));
		else
			assert(bio->vec = realloc(bio->vec, size));
		bio->cap = cap;
	}

	struct bio_vec *vec = &bio->vec[bio->cnt];

	vec->buf = buf;
	vec->offs = offs;
	vec->size = size;
	++bio->cnt;
}

void bio_submit(struct bio *bio)
{
	struct bdev *bdev = bio->bdev;

	assert(bdev);
	bdev->handle(bio);
}

void bio_wait(struct bio *bio)
{
	pthread_mutex_lock(&bio->mtx);
	while (!bio->handled)
		pthread_cond_wait(&bio->cv, &bio->mtx);
	pthread_mutex_unlock(&bio->mtx);
}

void bio_complete(struct bio *bio)
{
	pthread_mutex_lock(&bio->mtx);
	bio->handled = 1;
	pthread_cond_broadcast(&bio->cv);
	pthread_mutex_unlock(&bio->mtx);

	if (bio->complete)
		bio->complete(bio);
}
