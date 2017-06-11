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
#include <wal/wal.h>
#include <alloc/alloc.h>

#include <stdlib.h>
#include <string.h>
#include <assert.h>


struct __myfs_wal_jump {
	struct __myfs_wal_entry head;
	struct __myfs_wal_ptr ptr;
} __attribute__((packed));


static void myfs_wal_buf_setup(struct myfs_wal_buf *buf, size_t bytes)
{
	memset(buf, 0, sizeof(*buf));
	assert(buf->buf = calloc(1, bytes));
	buf->cap = bytes;
	assert(!pthread_mutex_init(&buf->mtx, NULL));
}

static void myfs_wal_buf_release(struct myfs_wal_buf *buf)
{
	assert(!pthread_mutex_destroy(&buf->mtx));
	free(buf->buf);
	memset(buf, 0, sizeof(*buf));
}

static void myfs_wal_buf_reset(struct myfs_wal_buf *buf)
{
	memset(buf->buf, 0, buf->cap);
	buf->size = 0;
	buf->offs = 0;
}


void myfs_wal_setup(struct myfs_wal *wal, struct myfs *myfs)
{
	static const size_t default_size = (size_t)1024 * 4096;

	memset(wal, 0, sizeof(*wal));
	myfs_wal_buf_setup(&wal->buf[0], default_size);
	myfs_wal_buf_setup(&wal->buf[1], default_size);

	wal->myfs = myfs;

	wal->current = &wal->buf[0];
	wal->next = &wal->buf[1];

	list_setup(&wal->wait_current);
	list_setup(&wal->wait_next);
	assert(!pthread_spin_init(&wal->lock, 0));
}

void myfs_wal_release(struct myfs_wal *wal)
{
	myfs_wal_buf_release(&wal->buf[1]);
	myfs_wal_buf_release(&wal->buf[0]);
	memset(wal, 0, sizeof(*wal));
}


void myfs_wal_trans_setup(struct myfs_trans *trans)
{
	memset(trans, 0, sizeof(*trans));
	assert(!pthread_mutex_init(&trans->mtx, NULL));
	assert(!pthread_cond_init(&trans->cv, NULL));
}

void myfs_wal_trans_release(struct myfs_trans *trans)
{
	free(trans->buf);
	assert(!pthread_cond_destroy(&trans->cv));
	assert(!pthread_mutex_destroy(&trans->mtx));
}

void myfs_wal_trans_append(struct myfs_trans *trans,
			const void *data, size_t size)
{
	if (!trans->buf) {
		static const size_t default_cap = 64 * 1024;

		assert(trans->buf = malloc(default_cap));
		trans->entry = trans->buf;
		trans->cap = default_cap;
		trans->size = sizeof(*trans->entry);
	}

	const size_t required = trans->size + size;

	if (required > trans->cap) {
		const size_t new_cap = trans->cap * 2 > required
					? trans->cap * 2 : required;

		assert(trans->buf = realloc(trans->buf, new_cap));
		trans->cap = new_cap;
	}

	memcpy((char *)trans->buf + trans->size, data, size);
	trans->size += size;
}

void myfs_wal_trans_finish(struct myfs_trans *trans)
{
	struct __myfs_wal_entry *entry = trans->entry;

	if (!entry)
		return;

	entry->size = htole32(trans->size);
	entry->type = htole32(MYFS_WAL_ENTRY);
	entry->csum = htole32(0);
	entry->csum = htole32(myfs_hash(entry, trans->size));
}


static void __myfs_wal_append(struct myfs_wal_buf *buf,
			const void *data, size_t size)
{
	memcpy((char *)buf->buf + buf->size, data, size);
	buf->size += size;
}

static void __myfs_wal_wait(struct myfs_trans *trans)
{
	assert(!pthread_mutex_lock(&trans->mtx));
	while (trans->wait)
		assert(!pthread_cond_wait(&trans->cv, &trans->mtx));
	assert(!pthread_mutex_unlock(&trans->mtx));
}

static void __myfs_wal_notify(struct list_head *wait)
{
	struct list_head *head = wait;

	for (struct list_head *ptr = head->next; ptr != head;) {
		struct myfs_trans *trans = (struct myfs_trans *)ptr;

		ptr = ptr->next;

		assert(!pthread_mutex_lock(&trans->mtx));
		trans->wait = 0;
		assert(!pthread_cond_signal(&trans->cv));
		assert(!pthread_mutex_unlock(&trans->mtx));
	}
}

static int __myfs_wal_allocate(struct myfs *myfs, struct myfs_wal_buf *buf)
{
	const uint64_t size = buf->cap / myfs->page_size;
	int err = 0;

	assert(!pthread_mutex_lock(&buf->mtx));
	if (!buf->offs)
		err = myfs_reserve(myfs, size, &buf->offs);
	assert(!pthread_mutex_unlock(&buf->mtx));
	return err;
}

static void __myfs_wal_link(struct myfs *myfs, struct myfs_wal_buf *prev,
			struct myfs_wal_buf *next)
{
	const size_t page_size = myfs->page_size;

	struct __myfs_wal_jump jump;

	assert(prev->cap - prev->size >= sizeof(jump));

	jump.head.size = htole32(sizeof(jump));
	jump.head.csum = htole32(0);
	jump.head.type = htole32(MYFS_WAL_JUMP);
	jump.ptr.offs = htole64(next->offs);
	jump.ptr.size = htole32(next->cap / page_size);
	jump.head.csum = htole32(myfs_hash(&jump, sizeof(jump)));
	__myfs_wal_append(prev, &jump, sizeof(jump));
}

int myfs_wal_append(struct myfs_wal *wal, struct myfs_trans *trans)
{
	struct myfs *myfs = wal->myfs;
	struct __myfs_wal_jump jump;
	struct myfs_wal_buf *current;
	struct myfs_wal_buf *next;
	struct list_head wait;
	int err = 0;

	while (1) {
		assert(!pthread_spin_lock(&wal->lock));
		current = wal->current;
		next = wal->next;

		if (wal->err) {
			err = wal->err;
			assert(!pthread_spin_unlock(&wal->lock));	
			return err;
		}

		if (!current) {
			trans->wait = 1;
			list_append(&wal->wait_current, &trans->ll);
			assert(!pthread_spin_unlock(&wal->lock));
			__myfs_wal_wait(trans);
			continue;
		}

		const size_t space = current->cap - current->size;
		const size_t required = trans->size + sizeof(jump);

		if (required <= space) {
			__myfs_wal_append(current, trans->entry, trans->size);
			assert(!pthread_spin_unlock(&wal->lock));
			break;
		}

		wal->current = next;
		wal->next = NULL;

		if (!next) {
			/**
			 * Congestion: both buffers are busy. Wait while the
			 * next buffer will be prepared, and make it current
			 * buffer as soon as possible, and delay all long
			 * stuff until the spinlock released.
			 **/
			trans->wait = 1;
			list_append(&wal->wait_next, &trans->ll);
			assert(!pthread_spin_unlock(&wal->lock));

			/* wait until the next buffer is ready */
			__myfs_wal_wait(trans);

			assert(!pthread_spin_lock(&wal->lock));

			if (wal->err) {
				err = wal->err;
				assert(!pthread_spin_unlock(&wal->lock));	
				return err;
			}

			wal->current = next = wal->next;
			wal->next = NULL;
			list_setup(&wait);
			list_splice(&wal->wait_current, &wait);
			__myfs_wal_append(next, trans->entry, trans->size);
			assert(!pthread_spin_unlock(&wal->lock));

			/* notify that the current buffer is ready */
			__myfs_wal_notify(&wait);
		} else {
			__myfs_wal_append(next, trans->entry, trans->size);
			assert(!pthread_spin_unlock(&wal->lock));	
		}

		/* we're responsible for linking buffers together */
		err = __myfs_wal_allocate(myfs, next);
		if (err)
			break;

		err = __myfs_wal_allocate(myfs, current);
		if (err)
			break;

		__myfs_wal_link(myfs, current, next);
		err = myfs_block_write(myfs, current->buf, current->cap,
					current->offs * myfs->page_size);
		if (err)
			break;

		myfs_wal_buf_reset(current);

		assert(!pthread_spin_lock(&wal->lock));
		list_setup(&wait);
		list_splice(&wal->wait_next, &wait);
		wal->next = current;
		assert(!pthread_spin_unlock(&wal->lock));

		__myfs_wal_notify(&wait);
		break;
	}

	if (err) {
		assert(!pthread_spin_lock(&wal->lock));
		if (!wal->err)
			wal->err = err;
		list_setup(&wait);
		list_splice(&wal->wait_next, &wait);
		list_splice(&wal->wait_current, &wait);
		assert(!pthread_spin_unlock(&wal->lock));
		__myfs_wal_notify(&wait);
	}
	return err;
}
