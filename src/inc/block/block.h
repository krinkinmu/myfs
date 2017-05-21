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
#ifndef __BLOCK_H__
#define __BLOCK_H__


#include <pthread.h>
#include <stdint.h>


#define BIO_RWDIR	(1ul << 0)
#define BIO_WRITE	(1ul << 0)
#define BIO_READ	(0ul)
#define BIO_SYNC	(1ul << 1)


struct bio_vec {
	void *buf;
	uint64_t offs;
	uint64_t size;
};

struct bio {
	pthread_mutex_t mtx;
	pthread_cond_t cv;
	int handled;

	unsigned long flags;
	int err;

	struct bdev *bdev;
	void (*complete)(struct bio *);

	size_t cnt, cap;
	struct bio_vec *vec;

	struct bio_vec _inline[8];
};

struct bdev {
	void (*handle)(struct bio *);
	size_t (*size)(struct bdev *);
};

struct sync_bdev {
	struct bdev bdev;
	int fd;
};

void sync_bdev_setup(struct sync_bdev *bdev, int fd);
size_t bdev_size(struct bdev *bdev);

void bio_setup(struct bio *bio, struct bdev *bdev);
void bio_release(struct bio *bio);

void bio_add_vec(struct bio *bio, void *buf, uint64_t offs, uint64_t size);
void bio_submit(struct bio *bio);
void bio_wait(struct bio *bio);
void bio_complete(struct bio *bio);

#endif /*__BLOCK_H__*/
