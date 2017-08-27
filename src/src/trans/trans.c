#include <alloc/alloc.h>
#include <trans/trans.h>
#include <myfs.h>
#include <stdatomic.h>


struct __myfs_trans_hdr {
	le8_t type;
	le32_t size;
	le64_t csum;
} __attribute__((packed));

struct myfs_trans_hdr {
	uint8_t type;
	uint32_t size;
	uint64_t csum;
};


static void myfs_trans_hdr2mem(struct myfs_trans_hdr *mem,
			const struct __myfs_trans_hdr *disk)
{
	mem->type = le8toh(disk->type);
	if (mem->type == MYFS_TRANS_NONE)
		return;
	mem->size = le32toh(disk->size);
	mem->csum = le64toh(disk->csum);
}


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

static void myfs_trans_entry2mem(struct myfs_trans_entry *mem,
			const struct __myfs_trans_entry *disk)
{
	mem->type = le32toh(disk->type);
	mem->size = le32toh(disk->size);
}

void myfs_trans_setup(struct myfs_trans *trans)
{
	memset(trans, 0, sizeof(*trans));
	assert(!pthread_mutex_init(&trans->mtx, NULL));
	assert(!pthread_cond_init(&trans->cv, NULL));
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

static void myfs_trans_notify(struct myfs_trans *trans, int status)
{
	assert(!pthread_mutex_lock(&trans->mtx));
	trans->status = status;
	assert(!pthread_cond_signal(&trans->cv));
	assert(!pthread_mutex_unlock(&trans->mtx));
}

void myfs_trans_submit(struct myfs *myfs, struct myfs_trans *trans)
{
	if (!trans->size) {
		myfs_trans_notify(trans, 1);
		return;
	}

	myfs_trans_finalize(trans);

	assert(!pthread_mutex_lock(&myfs->trans_mtx));
	int empty = list_empty(&myfs->trans);
	list_append(&myfs->trans, &trans->ll);
	if (empty)
		assert(!pthread_cond_signal(&myfs->trans_cv));
	assert(!pthread_mutex_unlock(&myfs->trans_mtx));
}

int myfs_trans_wait(struct myfs_trans *trans)
{
	assert(!pthread_mutex_lock(&trans->mtx));
	while (!trans->status)
		assert(!pthread_cond_wait(&trans->cv, &trans->mtx));
	assert(!pthread_mutex_unlock(&trans->mtx));
	return trans->status > 0 ? 0 : trans->status;
}

void myfs_trans_release(struct myfs_trans *trans)
{
	free(trans->data);
	assert(!pthread_mutex_destroy(&trans->mtx));
	assert(!pthread_cond_destroy(&trans->cv));
	memset(trans, 0, sizeof(*trans));
}


struct myfs_tx {
	char *start;
	size_t real_size;
	uint64_t offs;

	char *data;
	size_t size;
	size_t remain;
};

struct __myfs_tx_jump {
	struct __myfs_trans_hdr hdr;
	le64_t offs;
} __attribute__((packed));


static void myfs_tx_setup(struct myfs *myfs, struct myfs_tx *tx)
{
	size_t page_size = myfs->page_size;
	size_t pos = myfs->log.used;
	size_t aligned = myfs_align_down(pos, page_size);

	tx->start = myfs->log_data + aligned;
	tx->data = myfs->log_data + pos;
	tx->real_size = pos - aligned;
	tx->size = 0;
	tx->remain = MYFS_MAX_WAL_SIZE - pos;
	tx->offs = myfs->log.curr_offs + aligned / page_size;
}

static void myfs_tx_append(struct myfs_tx *tx, const void *data, size_t size)
{
	assert(size <= tx->remain);
	memcpy(tx->data + tx->size, data, size);
	tx->real_size += size;
	tx->size += size;
	tx->remain -= size;
}

static int myfs_tx_has_space(const struct myfs_tx *tx, struct myfs_trans *trans)
{
	return trans->size + sizeof(struct __myfs_tx_jump) <= tx->remain;
}

static void myfs_tx_jump(struct myfs_tx *tx, uint64_t offs)
{
	struct __myfs_tx_jump jump;

	jump.hdr.type = htole8(MYFS_TRANS_JUMP);
	jump.hdr.size = htole32(sizeof(jump));
	jump.hdr.csum = htole64(0);
	jump.offs = htole64(offs);
	jump.hdr.csum = htole64(myfs_csum(&jump, sizeof(jump)));
	myfs_tx_append(tx, &jump, sizeof(jump));
}

static int myfs_tx_commit(struct myfs *myfs, struct myfs_tx *tx, int force_next)
{
	int new_tx = force_next || tx->remain < myfs->page_size;
	uint32_t page_size = myfs->page_size;
	uint32_t pages = MYFS_MAX_WAL_SIZE / page_size;
	struct myfs_log_sb log = myfs->log;
	struct myfs_log_sb old = log;
	uint64_t offs;
	int err;

	if (new_tx) {
		err = myfs_reserve(myfs, pages, &offs);
		if (err)
			return err;

		myfs_tx_jump(tx, offs);
	}

	err = myfs_block_write(myfs, tx->data,
				myfs_align_up(tx->size, page_size),
				tx->offs * page_size);
	if (err)
		return err;

	if (new_tx) {
		log.curr_offs = offs;
		log.used = 0;
	} else {
		log.used += tx->size;
	}

	myfs->log = log;
	err = myfs_commit(myfs);
	if (err)
		myfs->log = old;
	return err;
}

static int myfs_trans_apply(struct myfs *myfs, const char *data, size_t size)
{
	while (size) {
		const struct __myfs_trans_entry *__entry =
					(struct __myfs_trans_entry *)data;
		struct myfs_trans_entry entry;
		int err;

		myfs_trans_entry2mem(&entry, __entry);
		err = myfs->trans_apply->apply(myfs, entry.type,
					data + sizeof(__entry), entry.size);
		if (err)
			return err;
		data += sizeof(__entry) + entry.size;
		size -= sizeof(__entry) + entry.size;
	}

	return 0;
}

static int myfs_tx_apply(struct myfs *myfs, struct myfs_tx *tx)
{
	size_t size = tx->size;
	char *data = tx->data;

	while (size) {
		struct __myfs_trans_hdr *__hdr =
					(struct __myfs_trans_hdr *)data;
		struct myfs_trans_hdr hdr;
		int err;

		if (htole8(__hdr->type) == MYFS_TRANS_NONE) {
			++data;
			--size;
			continue;
		}

		myfs_trans_hdr2mem(&hdr, __hdr);
		__hdr->csum = htole64(0);

		if (hdr.csum != myfs_csum(__hdr, hdr.size))
			return -1;

		if (hdr.type == MYFS_TRANS_JUMP)
			return 0;

		err = myfs_trans_apply(myfs, data + sizeof(*__hdr),
					hdr.size - sizeof(*__hdr));
		if (err)
			return err;

		data += hdr.size;
		size -= hdr.size;
		
	}
	return 0;
}


static void myfs_trans_batch(struct myfs *myfs, struct list_head *lst)
{
	struct list_head *head = lst;
	struct list_head *from = head->next;
	int err;

	while (from != head) {
		struct list_head *to = from;
		struct myfs_trans *trans = (struct myfs_trans *)to;
		struct myfs_tx tx;

		myfs_tx_setup(myfs, &tx);
		while (to != head && myfs_tx_has_space(&tx, trans)) {
			myfs_tx_append(&tx, trans->data, trans->size);
			to = to->next;
			trans = (struct myfs_trans *)to;
		}

		if ((err = myfs_tx_commit(myfs, &tx, to != head)))
			break;

		if ((err = myfs_tx_apply(myfs, &tx)))
			break;

		err = 1;
		for (struct list_head *p = from; p != to;) {
			struct myfs_trans *trans = (struct myfs_trans *)p;

			p = p->next;
			myfs_trans_notify(trans, err);
		}
		from = to;
	}

	for (struct list_head *p = from; p != head;) {
		struct myfs_trans *trans = (struct myfs_trans *)p;

		p = p->next;
		myfs_trans_notify(trans, err);
	}
}

void myfs_trans_worker(struct myfs *myfs)
{
	assert(myfs->trans_apply);
	assert(myfs->trans_apply->apply);

	while (1) {
		struct list_head list;

		list_setup(&list);
		assert(!pthread_mutex_lock(&myfs->trans_mtx));
		while (1) {
			if (!list_empty(&myfs->trans))
				break;
			if (myfs->done)
				break;
			assert(!pthread_cond_wait(&myfs->trans_cv,
						&myfs->trans_mtx));
		}
		list_splice(&myfs->trans, &list);
		assert(!pthread_mutex_unlock(&myfs->trans_mtx));

		if (list_empty(&list))
			break;

		myfs_trans_batch(myfs, &list);
	}
}
