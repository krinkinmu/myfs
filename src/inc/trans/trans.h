#ifndef __TRANS_H__
#define __TRANS_H__

#include <pthread.h>
#include <types.h>
#include <misc/list.h>

#define MYFS_MAX_WAL_SIZE	((uint32_t)4 * 1024 * 1024)
#define MYFS_MAX_TRANS_SIZE	((uint32_t)256 * 1024)

#define MYFS_TRANS_NONE		0
#define MYFS_TRANS_ENTRY	1
#define MYFS_TRANS_JUMP		2


struct __myfs_log_sb {
	le64_t head_offs;
	le64_t curr_offs;
	le32_t used;  // in bytes
};

struct myfs_log_sb {
	uint64_t head_offs;
	uint64_t curr_offs;
	uint32_t used;
};


static inline void myfs_log_sb2disk(struct __myfs_log_sb *disk,
			const struct myfs_log_sb *mem)
{
	disk->head_offs = htole64(mem->head_offs);
	disk->curr_offs = htole64(mem->curr_offs);
	disk->used = htole32(mem->used);
}

static inline void myfs_log_sb2mem(struct myfs_log_sb *mem,
			const struct __myfs_log_sb *disk)
{
	mem->head_offs = le64toh(disk->head_offs);
	mem->curr_offs = le64toh(disk->curr_offs);
	mem->used = le32toh(disk->used);
}

struct myfs_trans_apply {
	int(*apply)(struct myfs *, uint32_t, const void *, size_t);
};

struct myfs_trans {
	struct list_head ll;

	struct __myfs_trans_hdr *hdr;
	char *data;
	size_t size, cap;

	int status;
	pthread_mutex_t mtx;
	pthread_cond_t cv;
};

void myfs_trans_setup(struct myfs_trans *trans);
void myfs_trans_release(struct myfs_trans *trans);

void myfs_trans_append(struct myfs_trans *trans, uint32_t type,
			const void *data, size_t size);
void myfs_trans_submit(struct myfs *myfs, struct myfs_trans *trans);
int myfs_trans_wait(struct myfs_trans *trans);

void myfs_trans_worker(struct myfs *myfs);

#endif /*__TRANS_H__*/
