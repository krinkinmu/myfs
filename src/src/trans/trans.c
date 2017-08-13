#include <alloc/alloc.h>
#include <trans/trans.h>
#include <myfs.h>
#include <stdatomic.h>


struct __myfs_trans_hdr {
	le8_t type;
	le32_t size;
	le64_t csum;
} __attribute__((packed));

struct __myfs_trans_entry {
	le32_t type;
	le32_t size;
} __attribute__((packed));

struct myfs_trans_entry {
	uint32_t type;
	uint32_t size;
};

static void myfs_trans_entry2disk(struct __myfs_trans_entry *disk,
			const struct myfs_trans_entry *mem)
{
	disk->type = htole32(mem->type);
	disk->size = htole32(mem->size);
}

/*
static void myfs_trans_entry2mem(struct myfs_trans_entry *mem,
			const struct __myfs_trans_entry *disk)
{
	mem->type = le32toh(disk->type);
	mem->size = le32toh(disk->size);
}
*/

void myfs_trans_setup(struct myfs_trans *trans)
{
	memset(trans, 0, sizeof(*trans));
	assert(!pthread_mutex_init(&trans->mtx, NULL));
	assert(!pthread_cond_init(&trans->cv, NULL));
}

void myfs_trans_release(struct myfs_trans *trans)
{
	free(trans->data);
	assert(!pthread_mutex_destroy(&trans->mtx));
	assert(!pthread_cond_destroy(&trans->cv));
	memset(trans, 0, sizeof(*trans));
}

void myfs_trans_append(struct myfs_trans *trans, uint32_t type,
			const void *data, size_t size)
{
	size_t ent_size = size + sizeof(struct __myfs_trans_entry);
	size_t hdr_size = sizeof(struct __myfs_trans_hdr);
	size_t required = trans->size
				? trans->size + ent_size
				: ent_size + hdr_size;

	if (required > trans->cap) {
		size_t cap = trans->cap ? trans->cap * 2 : required;

		if (cap < required)
			cap = required;

		assert(trans->data = realloc(trans->data, cap));
		memset(trans->data + trans->cap, 0, cap - trans->cap);
		trans->cap = cap;
	}

	if (!trans->size) {
		trans->hdr = (struct __myfs_trans_hdr *)trans->data;
		trans->size += hdr_size;
	}

	struct __myfs_trans_entry disk;
	struct myfs_trans_entry mem;

	mem.type = type;
	mem.size = size;
	myfs_trans_entry2disk(&disk, &mem);
	memcpy(trans->data + trans->size, &disk, sizeof(disk));
	trans->size += sizeof(disk);
	memcpy(trans->data + trans->size, data, size);
	trans->size += size;
}

static void myfs_trans_finalize(struct myfs_trans *trans)
{
	assert(trans->size <= MYFS_MAX_TRANS_SIZE);
	trans->hdr->type = htole8(MYFS_TRANS_ENTRY);
	trans->hdr->size = htole32(trans->size);
	trans->hdr->csum = htole64(0);
	trans->hdr->csum = htole64(myfs_csum(trans->data, trans->size));
}

void myfs_trans_submit(struct myfs *myfs, struct myfs_trans *trans)
{
	struct myfs_trans *next;

	if (!trans->size) {
		trans->status = 1;
		return;
	}

	myfs_trans_finalize(trans);
	trans->status = 0;

	next = atomic_load_explicit(&myfs->trans, memory_order_relaxed);
	do {
		trans->next = next;
	} while (!atomic_compare_exchange_strong_explicit(&myfs->trans,
				&next, trans,
				memory_order_release, memory_order_relaxed));

	if (!next) {
		assert(!pthread_mutex_lock(&myfs->trans_mtx));
		assert(!pthread_cond_signal(&myfs->trans_cv));
		assert(!pthread_mutex_unlock(&myfs->trans_mtx));
	}
}

int myfs_trans_wait(struct myfs_trans *trans)
{
	assert(!pthread_mutex_lock(&trans->mtx));
	while (!trans->status)
		assert(!pthread_cond_wait(&trans->cv, &trans->mtx));
	assert(!pthread_mutex_unlock(&trans->mtx));
	return trans->status > 0 ? 0 : trans->status;
}


struct myfs_wal_buf {
	char *buf;
	size_t size;
	size_t remain;
	uint64_t offs;
};

static void myfs_wal_setup(struct myfs *myfs, struct myfs_wal_buf *buf)
{
	size_t pos = myfs->wal.used * myfs->page_size;

	buf->buf = myfs->wal_buf + pos;
	buf->size = 0;
	buf->remain = MYFS_MAX_WAL_SIZE - pos;
	buf->offs = myfs->wal.curr_offs + myfs->wal.used;
}

static void myfs_wal_finish(struct myfs *myfs, struct myfs_wal_buf *buf)
{
	size_t size = myfs_align_up(buf->size, myfs->page_size);
	size_t remain = myfs_align_down(buf->remain, myfs->page_size);

	if (size != buf->size)
		memset(buf + buf->size, MYFS_TRANS_NONE, size - buf->size);
	buf->size = size;
	buf->remain = remain;
}

static void myfs_wal_append(struct myfs_wal_buf *buf, const void *data,
			size_t size)
{
	assert(size <= buf->remain);
	memcpy(buf->buf + buf->size, data, size);
	buf->size += size;
}


struct __myfs_wal_jump {
	struct __myfs_trans_hdr hdr;
	le64_t offs;
} __attribute__((packed));

static void myfs_wal_jump_setup(struct __myfs_wal_jump *disk, uint64_t offs)
{
	disk->hdr.type = htole8(MYFS_TRANS_JUMP);
	disk->hdr.size = htole32(sizeof(*disk));
	disk->hdr.csum = htole64(0);
	disk->offs = htole64(offs);
	disk->hdr.csum = htole64(myfs_csum(disk, sizeof(*disk)));
}

static void myfs_trans_notify(struct myfs_trans *trans, int status)
{
	assert(!pthread_mutex_lock(&trans->mtx));
	trans->status = status;
	assert(!pthread_cond_signal(&trans->cv));
	assert(!pthread_mutex_unlock(&trans->mtx));
}

static int myfs_wal_commit(struct myfs *myfs, const struct myfs_wal_sb *wal)
{
	struct myfs_wal_sb old = myfs->wal;
	int err;

	myfs->wal = *wal;
	if ((err = myfs_commit(myfs)))
		myfs->wal = old;
	return err;
}

static void myfs_trans_batch(struct myfs *myfs, struct myfs_trans *lst)
{
	uint32_t page_size = myfs->page_size;
	uint32_t pages = MYFS_MAX_WAL_SIZE / page_size;
	struct myfs_trans *head = lst;
	int err;

	while (head) {
		struct myfs_wal_sb wal = myfs->wal;
		struct myfs_trans *trans = head;
		struct __myfs_wal_jump jump;
		struct myfs_wal_buf buf;
		uint64_t offs;

		myfs_wal_setup(myfs, &buf);
		while (trans && trans->size + sizeof(jump) <= buf.remain) {
			myfs_wal_append(&buf, trans->data, trans->size);
			trans = trans->next;
		}

		// Check if we need to allocate a new WAL chunk
		int new_wal = trans || buf.remain < myfs->page_size;

		if (new_wal) {
			if ((err = myfs_reserve(myfs, pages, &offs)))
				goto fail_all;

			myfs_wal_jump_setup(&jump, offs);
			myfs_wal_append(&buf, &jump, sizeof(jump));
		}
		myfs_wal_finish(myfs, &buf);

		if ((err = myfs_block_write(myfs, buf.buf, buf.size,
					buf.offs * page_size)))
			goto fail_all;

		if (new_wal) {
			wal.curr_offs = offs;
			wal.used = 0;
		} else {
			wal.used += buf.size / page_size;
		}

		// TODO: calculate and allocate space required for replaying
		// or even better, the user that created transaction should do
		// that before commiting it, this way we wouldn't run out of
		// memory after commit (either way it wouldn't work for large
		// transactions though).
		if ((err = myfs_wal_commit(myfs, &wal)))
			goto fail_all;

		// TODO: implement replay with an interface like this:
		// if ((err = myfs_wal_replay(myfs, buf)))
		//	 goto fail_all;

		while (head != trans) {
			struct myfs_trans *prev = head;

			head = head->next;
			myfs_trans_notify(prev, 1);
		}
	}
	err = 1;

fail_all:
	while (head) {
		struct myfs_trans *trans = head;

		head = head->next;
		myfs_trans_notify(trans, err);
	}
}

static void myfs_trans_worker_wait(struct myfs *myfs)
{
	assert(!pthread_mutex_lock(&myfs->trans_mtx));
	while (1) {
		if (atomic_load_explicit(&myfs->trans, memory_order_relaxed))
			break;
		if (myfs->done)
			break;
		assert(!pthread_cond_wait(&myfs->trans_cv, &myfs->trans_mtx));
	}
	assert(!pthread_mutex_unlock(&myfs->trans_mtx));
}

void myfs_trans_worker(struct myfs *myfs)
{
	while (1) {
		struct myfs_trans *list = atomic_load_explicit(&myfs->trans,
					memory_order_relaxed);
		struct myfs_trans *rev = NULL;

		if (!list) {
			// slow path, acquire mutex and wait on condition var
			myfs_trans_worker_wait(myfs);
			list = atomic_load_explicit(&myfs->trans,
						memory_order_relaxed);
			if (!list)
				return;
		}

		while (!atomic_compare_exchange_strong_explicit(&myfs->trans,
					&list, NULL,
					memory_order_consume,
					memory_order_relaxed));

		// reverse the list to process transactions in the right order
		while (list) {
			struct myfs_trans *next = list->next;

			list->next = rev;
			rev = list;
			list = next;
		}

		myfs_trans_batch(myfs, rev);
	}
}
